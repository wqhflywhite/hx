# 两阶段提交读书笔记

coordinator 先收集全部参与者的 vote，全票赞成才发出提交决定；coordinator 一旦在决定前 crash，参与者只能 blocking 等待恢复。

- 阻塞窗口是协议的原生缺陷，需要超时加人工介入兜底。

上位概念：[[wiki/两阶段提交]]
