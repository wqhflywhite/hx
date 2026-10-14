---
source: "TiKV 设计文档阅读"
---
# TiKV 笔记

数据按 region 切分，region 过大即 split，每个 region 独立一个 raftstore 实例，由 pd 调度组件均衡热点。


参考：[[raw/etcd-notes]] [[raw/spanner]]
