---
confidence: high
sources: [raw/raft-paper.md]
---
# Raft

Raft 把共识拆成[[wiki/Leader选举]]与[[wiki/日志复制]]两个子问题，属于[[wiki/共识算法]]的一种。细节见[[raw/raft-paper]]。

## 设计要点

- 强领导者：日志只从领导者流向跟随者。
- 任期编号作为逻辑时钟，拒绝过期领导者。
