#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <unordered_map>
#include "clerk.h"
#include "util.h"
#include "monsoon.h"  // 协程调度器
#include "mprpccontroller.h"  // RPC错误处理

// 业务常量定义
const int kTotalOps = 10000;          // 总操作次数
const int kHotKeyRatio = 20;          // 热点键占比（20%）
const int kMaxRetries = 3;            // 最大重试次数
const std::vector<std::string> kKeyPrefixes = {
    "user:",   // 用户信息键
    "goods:",  // 商品信息键
    "order:"   // 订单信息键
};

// 生成随机业务键（含热点键逻辑）
std::string generateKey(std::mt19937& rng) {
    std::uniform_int_distribution<int> prefixDist(0, kKeyPrefixes.size() - 1);
    std::string prefix = kKeyPrefixes[prefixDist(rng)];

    // 20%概率生成热点键（固定后缀），80%生成随机键
    std::uniform_int_distribution<int> hotDist(0, 99);
    if (hotDist(rng) < kHotKeyRatio) {
        return prefix + "hot";  // 热点键：如user:hot、goods:hot
    } else {
        std::uniform_int_distribution<int> idDist(1000, 9999);
        return prefix + std::to_string(idDist(rng));  // 随机键：如user:1234
    }
}

// 生成随机值（修复时间戳转换问题）
std::string generateValue(std::mt19937& rng) {
    std::uniform_int_distribution<int> valDist(100000, 999999);
    // 将 time_point 转换为毫秒级时间戳
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now().time_since_epoch()
    ).count();
    return "{\"data\":" + std::to_string(valDist(rng)) + ",\"ts\":" + std::to_string(now_ms) + "}";
}

// 业务操作类型
enum OpType { PUT, GET, DELETE };

// 执行单次业务操作（带重试逻辑）
bool executeOp(Clerk& client, OpType op, const std::string& key, const std::string& value, int opId) {
    MprpcController controller;  // 用于RPC错误检测
    for (int retry = 0; retry < kMaxRetries; ++retry) {
        controller.Reset();  // 重置错误状态
        try {
            switch (op) {
                case PUT: {
                    client.Put(key, value);
                    break;
                }
                case GET: {
                    std::string res = client.Get(key);
                    // 业务日志：记录Get结果
                    std::cout << "[Op" << opId << "] GET " << key << " -> " << (res.empty() ? "null" : res) << "\n";
                    break;
                }
                
            }
            // 无错误则返回成功
            return true;
        } catch (...) {
            // 捕获RPC或业务异常（需Clerk抛出异常或通过controller反馈）
            if (controller.Failed()) {
                std::cerr << "[Op" << opId << "] Retry " << retry + 1 << " failed: " << controller.ErrorText() << "\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));  // 指数退避重试
            } else {
                std::cerr << "[Op" << opId << "] Unknown error, retry " << retry + 1 << "\n";
            }
        }
    }
    std::cerr << "[Op" << opId << "] Max retries reached, operation failed\n";
    return false;
}

// 协程任务：模拟单个"用户"的一系列操作
void userTask(Clerk& client, int userId, int startOpId, int opCount) {
    // 每个用户独立随机数生成器（避免并发冲突）
    std::random_device rd;
    std::mt19937 rng(rd() + userId);  // 用userId偏移，保证随机性
    std::uniform_int_distribution<int> opDist(0, 2);  // 0:PUT, 1:GET, 2:DELETE

    for (int i = 0; i < opCount; ++i) {
        int opId = startOpId + i;
        OpType op = static_cast<OpType>(opDist(rng));
        std::string key = generateKey(rng);
        std::string value = generateValue(rng);

        // 执行操作并记录成功率
        executeOp(client, op, key, value, opId);

        // 模拟用户操作间隔（随机10-100ms）
        std::uniform_int_distribution<int> delayDist(10, 100);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayDist(rng)));
    }
}

int main(int argc, char**argv) {
    // 初始化客户端（从配置文件读取集群信息）
    Clerk client;
    client.Init("test.conf");
       

    // 配置并发参数
    int threadNum = 8;  // 模拟8个并发用户线程
    int opsPerThread = kTotalOps / threadNum;

    // 启动协程调度器（利用项目中的fiber框架提高并发效率）
    monsoon::IOManager iom(threadNum);  // 线程数=并发用户数
    auto start = now();

    // 分配任务到协程
    for (int i = 0; i < threadNum; ++i) {
        int userId = i;
        int startOpId = i * opsPerThread;
        int actualOps = (i == threadNum - 1) ? kTotalOps - startOpId : opsPerThread;

        iom.scheduler([&client, userId, startOpId, actualOps]() {
            userTask(client, userId, startOpId, actualOps);
        });
    }

    // 等待所有任务完成
    iom.stop();
    auto end = now();
  
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  double duration = duration_ms / 1000.0;  // 转换为秒

    // 输出业务统计
    std::cout << "\n=== 业务统计 ===" << "\n";
    std::cout << "总操作数: " << kTotalOps << "\n";
    std::cout << "总耗时: " << duration << "s\n";
    std::cout << "平均QPS: " << kTotalOps / duration << "\n";

    return 0;
}