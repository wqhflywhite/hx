---
source: "Ongaro & Ousterhout, In Search of an Understandable Consensus Algorithm, 2014"
---
# Raft 论文笔记

核心贡献：把共识拆成 election、log replication、safety 三个独立子问题。

- election timeout 取 randomized 区间，heartbeat 周期远小于 timeout，避免选票分裂。
- 只有当前任期的条目才能用 majority 计数推进 commit index。
- 换届时旧届 leader 的条目可能只留在已失效的多数上，贸然提交会与新届数据冲突；因此提交规则只信任本届选出的 leader 亲自写入的条目。


上位概念：[[wiki/Raft]]
