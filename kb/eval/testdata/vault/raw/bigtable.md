---
source: "Chang 等, Bigtable, 2006"
---
# Bigtable 论文笔记

结构化数据存储：表按行键切成 tablet，写入先进 memtable 再落成 sstable，compaction 周期性归并分层文件。

- 单行事务，跨行不保证。


依赖：[[raw/gfs]] [[raw/chubby]]
