---
source: "PostgreSQL 与 MySQL InnoDB 实现阅读"
---
# MVCC 笔记

每行挂一条 version chain，事务按 readview 判定 visibility，旧版本由后台回收。


隔离级别：[[raw/snapshot-isolation]]
