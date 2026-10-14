---
source: "Lamport, Paxos Made Simple, 2001"
---
# Paxos Made Simple 笔记

简明教程：proposer 先带 ballot 编号发起 prepare，acceptor 承诺不再接受更小编号；第二轮 accept 由 quorum 多数派落定取值。

- 活锁来自 proposer 之间 ballot 编号互相碾压。


上位概念：[[wiki/Paxos]]
