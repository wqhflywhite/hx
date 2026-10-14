---
source: "DeCandia 等, Dynamo, 2007"
---
# Dynamo 论文笔记

高可用键值存储：consistent hashing 把键映射到 vnode 环，故障时用 hinted handoff 暂存邻座，恢复后借 merkle tree 对账。

- 高可用键值存储 的代价：读也要凑够副本数才够新鲜。


上位概念：[[wiki/最终一致性]]

延伸阅读：[[raw/vector-clock]]
