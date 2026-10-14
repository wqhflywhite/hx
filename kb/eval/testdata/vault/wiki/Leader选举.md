---
confidence: high
sources: [raw/raft-paper.md]
---
# Leader选举

领导者由多数派投票产生，领导者的心跳压制其他候选者。[[wiki/Raft]]用随机化超时错开竞争。

## 脑裂防护

领导者失去多数派支持后必须退位；配合[[wiki/租约]]可以让读请求也只在领导者处服务。
