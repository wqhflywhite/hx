---
source: "LinkedIn, Kafka, 2011"
---
# Kafka 笔记

消息日志：topic 切成 partition，每条记录带单调 offset，broker 只管顺序读写，consumer 位移由客户端提交。

- 消息日志 的保留策略让重放成为一等能力。


抽象同源：[[wiki/日志复制]]
