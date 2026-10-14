---
source: "Berenson 等, A Critique of ANSI SQL Isolation Levels, 1995"
---
# 快照隔离笔记

读不加锁，写冲突先提交者胜；残余风险是 write skew anomaly：两个事务各读对方要改的行再各写各行。


上位概念：[[wiki/分布式事务]]

实现机制：[[raw/mvcc-notes]]
