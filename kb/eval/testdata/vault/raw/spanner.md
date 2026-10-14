---
source: "Corbett 等, Spanner, 2012"
---
# Spanner 笔记

全球数据库，靠原子钟加 GPS 把 clock 误差收进 TrueTime 的 uncertainty 区间；读写事务在 commit 前 wait 掉 uncertainty 窗口，换来外部一致。

- 全球数据库 的卖点：跨洲外部一致与不停机改 schema。


上位概念：[[wiki/时钟同步]] [[wiki/分布式事务]]
