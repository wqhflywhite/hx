---
confidence: medium
sources: [raw/paxos-made-simple.md]
---
# Paxos

Paxos 是最早在理论上解决[[wiki/共识算法]]的协议，原文笔记见[[raw/paxos-made-simple]]。

## 工程化

单轮 Paxos 只能确定一个值，连续确定多个值需要[[wiki/MultiPaxos]]。
