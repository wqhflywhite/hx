---
source: "Burrows, The Chubby Lock Service, 2006"
---
# Chubby 论文笔记

分布式锁服务：客户端用 keepalive 续约会话，写操作可附 sequencer 让后续持有者鉴别过期锁；锁语义是 advisory 建议性的。

- 分布式锁服务 的读多写少假设决定了粗粒度锁设计。


上位概念：[[wiki/Paxos]] [[wiki/租约]]
