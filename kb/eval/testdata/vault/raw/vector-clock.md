---
source: "分布式系统课程讲义"
---
# 向量时钟笔记

每个副本维护自己的计数，合并时取逐分量最大；无法定序的两个版本互为 sibling，causality 偏序由 dotted 表示压缩。


应用场景：[[raw/dynamo]]
