---
source: "Hunt 等, ZooKeeper, 2010"
---
# ZooKeeper 笔记

协调服务内核：zab 协议广播更新，ephemeral 节点随会话消亡，watch 一次性通知变更，zxid 全序所有写。

- 协调服务内核 暴露的是文件系统式的小数据原语。


对标：[[raw/chubby]]
