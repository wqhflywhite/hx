---
source: "Chandra 等, Paxos Made Live, 2007"
---
# Paxos Made Live 笔记

Google 工程实录：stable leader 之后只对首次做完整协商，后续条目走 pipelining，throughput 提升一个量级。

- 磁盘故障与 epoch 损坏是真实事故的主要来源。


上位概念：[[wiki/MultiPaxos]]
