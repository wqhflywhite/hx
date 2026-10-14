---
source: "Garcia-Molina & Salem, Sagas, 1987"
---
# Saga 论文笔记

长事务拆成一串本地事务，每一步注册 compensation；失败时按逆序 rollback。两种驱动方式：choreography 事件接力与 orchestration 集中编排。


上位概念：[[wiki/Saga]]
