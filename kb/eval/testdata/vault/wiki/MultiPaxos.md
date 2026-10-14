---
confidence: medium
sources: [raw/paxos-made-live.md]
---
# MultiPaxos

MultiPaxos 复用[[wiki/Paxos]]的协议消息，让一串值共享同一轮准备阶段，工程经验见[[raw/paxos-made-live]]。

## 关键收益

MultiPaxos 省去了每条的协商开销，是多数生产系统的实际形态。
