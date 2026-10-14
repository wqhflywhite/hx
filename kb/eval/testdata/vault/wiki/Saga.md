---
confidence: medium
sources: [raw/saga-paper.md]
---
# Saga

Saga 把一个[[wiki/分布式事务]]拆成一串本地事务，适用场景是跨服务的长流程：每一步都有逆向操作，原始论文见[[raw/saga-paper]]。

## 适用场景

Saga 放弃强一致换可用性，适合订单、审批这类可补偿业务。
