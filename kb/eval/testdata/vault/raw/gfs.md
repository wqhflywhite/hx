---
source: "Ghemawat 等, The Google File System, 2003"
---
# GFS 论文笔记

分布式文件系统：master 管元数据，chunkserver 管 64MB 大块；追加写用 record append 语义，由主副本定序。

- 分布式文件系统 的设计假设：追加远多于覆盖，带宽重于延迟。
