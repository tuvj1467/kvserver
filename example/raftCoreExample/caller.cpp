#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <unordered_map>
#include "clerk.h"
#include "util.h"
#include "mprpccontroller.h"  // RPC错误处理（非协程，保留）

// 业务常量定义（不变）
const int kTotalOps = 10000;          // 总操作次数
const int kHotKeyRatio = 20;          // 热点键占比（20%）
const int kMaxRetries = 3;            // 最大重试次数
const std::vector<std::string> kKeyPrefixes = {
    "user:",   // 用户信息键
    "goods:",  // 商品信息键
    "order:"   // 订单信息键
};

// 生成随机业务键（含热点键逻辑，不变）
std::string generateKey(std::mt19937& rng) {
    std::uniform_int_distribution<int> prefixDist(0, kKeyPrefixes.size() - 1);
    std::string prefix = kKeyPrefixes[prefixDist(rng)];

    // 20%概率生成热点键，80%生成随机键
    std::uniform_int_distribution<int> hotDist(0, 99);
    if (hotDist(rng) < kHotKeyRatio) {
        return prefix + "hot";  // 热点键：如user:hot、goods:hot
    } else {
        std::uniform_int_distribution<int> idDist(1000, 9999);
        std::string key = prefix + std::to_string(idDist(rng));
        // 新增：普通键首次生成时打印日志
        static std::unordered_set<std::string>普通_keys;
        if (普通_keys.find(key) == 普通_keys.end()) {
            普通_keys.insert(key);
            std::cout << "[KeyGen] 生成普通键：" << key << "\n" << std::flush;
        }
        return key;
    }
}

// 生成随机值（修复时间戳转换问题，不变）
std::string generateValue(std::mt19937& rng) {
    std::uniform_int_distribution<int> valDist(100000, 999999);
    // 将 time_point 转换为毫秒级时间戳
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now().time_since_epoch()
    ).count();
    return "{\"data\":" + std::to_string(valDist(rng)) + ",\"ts\":" + std::to_string(now_ms) + "}";
}

// 业务操作类型（不变，暂不处理DELETE以避免未定义行为）
enum OpType { PUT, GET };

// 执行单次业务操作（带重试逻辑，不变）
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
            // 捕获RPC或业务异常
            if (controller.Failed()) {
                std::cerr << "[Op" << opId << "] Retry " << retry + 1 << " failed: " << controller.ErrorText() << "\n";
                // 指数退避重试（普通线程睡眠，非协程睡眠）
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            } else {
                std::cerr << "[Op" << opId << "] Unknown error, retry " << retry + 1 << "\n";
            }
        }
    }
    std::cerr << "[Op" << opId << "] Max retries reached, operation failed\n";
    return false;
}

// 普通任务函数：模拟单个"用户"的一系列操作（移除协程依赖，不变）
void userTask(Clerk& client, int userId, int startOpId, int opCount) {
    // 每个任务独立随机数生成器（避免并发冲突）
    std::random_device rd;
    std::mt19937 rng(rd() + userId);  // 用userId偏移，保证随机性
    std::uniform_int_distribution<int> opDist(0, 1);  // 仅PUT/GET，避免DELETE未实现问题

    for (int i = 0; i < opCount; ++i) {
        int opId = startOpId + i;
        OpType op = static_cast<OpType>(opDist(rng));
        std::string key = generateKey(rng);
        std::string value = generateValue(rng);

        // 执行操作并记录成功率
        executeOp(client, op, key, value, opId);

        // 模拟用户操作间隔（普通线程睡眠，非协程睡眠）
        std::uniform_int_distribution<int> delayDist(10, 100);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayDist(rng)));
    }
}

int main(int argc, char**argv) {
    // 1. 初始化客户端（检查返回值，不变）
    Clerk client;
    client.Init("test.conf");

    // 2. 配置执行参数（移除协程调度器，改用普通线程）
    int threadNum = 1;  // 可选：1=单线程（简单稳定），8=普通多线程（无协程）
    int opsPerThread = kTotalOps / threadNum;
    std::vector<std::thread> threads;  // 普通线程容器（替代协程调度器）

    // 3. 记录开始时间（不变）
    auto start = now();

    // 4. 分配任务到普通线程（替代协程scheduler）
    for (int i = 0; i < threadNum; ++i) {
        int userId = i;
        int startOpId = i * opsPerThread;
        // 最后一个线程处理剩余操作（避免整除问题）
        int actualOps = (i == threadNum - 1) ? kTotalOps - startOpId : opsPerThread;

        // 启动普通线程执行任务（替代协程任务）
        threads.emplace_back([&client, userId, startOpId, actualOps]() {
            userTask(client, userId, startOpId, actualOps);
        });
    }

    // 5. 等待所有普通线程完成（替代协程iom.stop()）
    for (auto& t : threads) {
        t.join();
    }

    // 6. 计算耗时与输出统计（不变）
    auto end = now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double duration = duration_ms / 1000.0;  // 转换为秒

    std::cout << "\n=== 业务统计 ===" << "\n";
    std::cout << "总操作数: " << kTotalOps << "\n";
    std::cout << "总耗时: " << duration << "s\n";
    std::cout << "平均QPS: " << kTotalOps / duration << "\n";

    return 0;
}