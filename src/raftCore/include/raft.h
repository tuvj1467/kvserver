#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "util.h"
/// @brief //////////// 网络状态表示  todo：可以在rpc中删除该字段，实际生产中是用不到的.
constexpr int Disconnected =
    0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

///////////////投票状态

constexpr int Killed = 0;
constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 private:
  std::mutex m_mtx;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
  std::shared_ptr<Persister> m_persister;
  int m_me;                                       // 当前节点ID
  int m_currentTerm;
  int m_votedFor;
  std::vector<raftRpcProctoc::LogEntry> m_logs;  // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
                                                 // 这两个状态所有结点都在维护，易失
  int m_commitIndex;
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log 的index

  // 这两个状态是由服务器来维护，易失
  std::vector<int>m_nextIndex;  // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
  std::vector<int> m_matchIndex;

  enum Status { Follower, Candidate, Leader };
  // 身份
  Status m_status;

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  // client从这里取日志（2B），client与raft通信的接口
  

  // 选举超时
  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
  // 心跳超时，用于leader
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

  // 2D中用于传入快照点
  // 储存了快照中的最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  // 协程
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

 public:
  //处理AppendEntries RPC的核心逻辑
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);

   //日志应用定时器
   //功能：定期检查已提交但未应用的日志，将其封装为ApplyMsg并推送到applyChan，供上层应用处理
  void applierTicker();

  /**
   * @brief 条件性安装快照
   * @param lastIncludedTerm 快照包含的最后日志的任期
   * @param lastIncludedIndex 快照包含的最后日志的索引
   * @param snapshot 快照数据
   * @return 是否成功安装快照
   * 功能：检查快照是否比本地状态更新，若符合条件则替换本地快照和日志（截断旧日志）
   */
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);


  void doElection();
  
  //发起心跳，只有leader才需要发起心跳
  void doHeartBeat();

  // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了,负责监控节点是否长时间未收到 Leader 的心跳（或有效的 RPC），并在超时后触发新的选举（doElection()
  // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
  void electionTimeOutTicker();

  /**
   * @brief 获取需应用的日志
   * @return 待应用的日志条目列表（封装为ApplyMsg）
   * 功能：收集已提交但未应用的日志，供上层应用获取
   */
  std::vector<ApplyMsg> getApplyLogs();

  //获取新命令的日志索引
  int getNewCommandIndex();
  //获取指定服务器的前序日志信息
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);
  //获取当前节点状态
  void GetState(int *term, bool *isLeader);
  //处理InstallSnapshot RPC  Follower 日志落后于 Leader 过多（甚至落后于 Leader 已压缩的快照）时，通过接收并安装 Leader 的快照快照来快速同步状态
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);
  //定期触发领导者发送心跳（调用doHeartBeat），维持领导地位
  void leaderHearBeatTicker();
  //领导者向指定服务器发送快照,当跟随者日志落后过多时，领导者直接发送快照而非逐条日志，减少数据传输
  void leaderSendSnapShot(int server);
  //领导者更新提交索引
  void leaderUpdateCommitIndex();
  //验证日志是否匹配
  bool matchLog(int logIndex, int logTerm);

  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
  //判断候选者日志是否更新
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  int getLastLogTerm();

  //同时获取最后一条日志的索引和任期
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
  //根据日志索引获取对应的任期
  int getLogTermFromLogIndex(int logIndex);

  //获取持久化的Raft状态大小
  int GetRaftStateSize();
  //将日志全局索引转换为本地日志数组的下标
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

  //将ApplyMsg推送到KV服务
  void pushMsgToKvServer(ApplyMsg msg);
  //从持久化数据恢复Raft状态
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

  
  // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
  // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
  // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
  void Snapshot(int index, std::string snapshot);

 public:
  // 重写基类方法,因为rpc远程调用真正调用的是这个方法
  //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

 private:
  // for persist

  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar &m_currentTerm;
      ar &m_votedFor;
      ar &m_lastSnapshotIncludeIndex;
      ar &m_lastSnapshotIncludeTerm;
      ar &m_logs;
    }
    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;

   public:
  };
};

#endif  // RAFT_H