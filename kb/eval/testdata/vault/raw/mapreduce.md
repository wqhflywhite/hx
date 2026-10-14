---
source: "Dean & Ghemawat, MapReduce, 2004"
---
# MapReduce 论文笔记

批处理模型：map 产出 intermediate 键值对，框架按键 shuffle 到 reducer，combiner 在本地先聚合削减传输。

- 批处理模型 的容错靠重跑任务而非复制。


存储底座：[[raw/gfs]]
