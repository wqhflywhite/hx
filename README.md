# hx — Go 重写的 Agent Harness 内核

hx 是对 DeepSeek Harness（dsh，`deepseek-ai/deepseek-harness`）架构脊柱的个人 Go 重写。它复刻 dsh 验证过的五条设计不变量，甩掉 Node 生态的结构性成本：单静态二进制、`go build` 一条路、**直接依赖仅 5 个**。

技术方案见 [../GO-REWRITE-PLAN.md](../GO-REWRITE-PLAN.md)。

## 设计血统：从 dsh 保留的五条不变量

1. **注册即可逆副作用**：每个 register 返回 disposer，卸载时逆序、幂等、级联撤销。
2. **声明式依赖激活**：插件声明 inject，服务就绪自动激活，不手动编排启动顺序。
3. **能力 seam 三角色**：Definition / Provider / Consumer，换 provider 即换产品行为。
4. **会话日志是模型上下文的唯一来源**：模型可见 ⟺ 已落日志；历史从日志投影。
5. **组合是声明式的**：配置列出插件、按 id patch 覆盖；loop 本身也是可替换插件。

## 架构总览

```
┌──────────────────────────────────────────────────┐
│ 界面层（普通插件，可互换）     cli · tui           │
├──────────────────────────────────────────────────┤
│ agentloop / oneshotloop ── 可替换 ReAct 驱动器    │
│ approval ── tools/pre-execute 审批监听器          │
│ subagent ── delegate 工具 + 委派深度闸            │
│ compaction ── 上下文压缩 Provider                 │
│ memory ── 长期记忆注入（durable history）         │
│ kb ── 知识库（vault 索引 + 检索/受控写入工具）      │
│ toolgate ── 工具闸门（max_inline + tool_search）    │
├──────────────────────────────────────────────────┤
│ agent ── Agent 注册表 + Factory seam             │
├──────────┬──────────┬───────────┬────────────────┤
│ session  │ llm      │ tools     │ prompt         │
│ 日志+投影 │ 命名适配器│ 注册表+流水线│ 提示词组装      │
├──────────┴──────────┴───────────┴────────────────┤
│ core/fiber · core/event · core/plugin            │
│ hxconfig ── hx.yml 组合 + 双层 Stack 热重挂        │
└──────────────────────────────────────────────────┘
```

## 包清单（18,539 行，其中测试 9,268 行）

### 内核三层（M1）

| 包 | 行数 | 职责 |
|---|---|---|
| `core/fiber` | 437 | 可逆副作用容器树。`Effect` 是唯一注册入口；`Provide[T]`/`Get[T]` 泛型服务存取（祖先链优先，普通 fiber 回退全树、generation 只搜索本代与 pinned 区域）；`Dispose` 幂等、LIFO、级联子 fiber，并发调用等待同一清理结果；`Done` channel 供插件 goroutine 挂靠生命周期 |
| `core/event` | 223 | 泛型事件总线。`Emit`（同步广播）、`Parallel`（扇出聚合错误）、`Serial`（bail 即停）、`Waterfall`（洋葱中间件并贯穿 context，不调 next 即短路）；注册时记录 payload 类型，同名冲突挂载即 panic，Effect 失败回滚监听器 |
| `core/plugin` | 106 | 编译期注册表 + inject 激活。依赖未就绪时 fiber 异步等待（generation 条件变量），`Provide` 唤醒；服务撤销时经 `OnRevoked` 钩子级联 Dispose 依赖方 |

### 能力 seam（M2）

| 包 | 行数 | 职责 |
|---|---|---|
| `session` | 876 | JSONL 追加日志（`~/.hx/sessions/`，env `HX_SESSIONS_DIR` 或 config `dir` 覆盖）。同会话所有 Resume 句柄共享顺序锁，写盘、fsync、缓存更新和冷读顺序一致；`Event.Data` 在输入、返回和广播边界深拷贝；读取无固定行上限，坏后缀在 Resume 时物理截断并 fsync。`DeriveMessages` 是全量投影定义，summary 以 `ThroughSeq` 标记水位、`FirstKept` 钉住头部，prune 只改模型视图；增量 fold 与全量逐事件等价，`Fork` 复制前缀，时钟与 id 生成器均可注入 |
| `llm` | 190 | 适配器 seam。**命名注册表**：`SetAdapter(name, a)` 支持热重挂替换与逆序回滚，旧 disposer 不误删新实例；`Request.Provider` 按名路由，空名走默认；`Stream` 经带 ctx 的 `llm/stream` waterfall |
| `llm/deepseek` | 342 | OpenAI 兼容 provider：`POST {base_url}/chat/completions`，`net/http` + 手写 SSE 解析；只有显式 `[DONE]` 是正常终态，异常 EOF、错误帧与取消均向上报错，稀疏 tool_call index 按实际 key 排序；config `base_url`/`api_key`/`model`/`name`，env 回退 `DEEPSEEK_*` |
| `tools` | 146 | 工具注册表（`Register` 返 disposer）+ 执行流水线：`tools/pre-execute` → execute → `tools/post-execute`，`context.Context` 贯穿瀑布和工具实现，结果由调用方落日志 |
| `tools/builtin` | 467 | 内置工具：`bash`（Windows `cmd /C` / Unix `sh -c`，继承 turn ctx、超时、进程组/Job Object/进程树回退清理、流式有界输出）、`read_file`（LimitReader 硬上限）、`write_file`、`edit` |
| `prompt` | 135 | 提示词分片（`Section{Name,Order,Render}`）注册返 disposer；`Assemble` 按 `(Order,Name)` 确定顺序拼系统提示词并收集 tools schema；`prompt/tools` waterfall 供 toolgate 裁剪内联集合 |
| `agent` | 87 | Agent 注册表 + **Factory seam**：注册表不创建 agent，`SetFactory` 返 disposer、重复注册 panic；未注册工厂时报明确错误 |

### 驱动器（M2 + M4c）

| 包 | 行数 | 职责 |
|---|---|---|
| `agentloop` | 293 | 默认 ReAct 驱动器。turn/step 语义；取消走 `context.Context` + fiber done 双轨并贯穿工具/审批；**一切模型可见内容先落日志再广播**；流错误不留下悬空 tool calls；Dispose 等待 run 退出再关日志；每 step 可选调用 compaction |
| `agentloop/oneshot` | 263 | 替代驱动器：一步带工具 + 一步无工具收尾。配置里把 `agentloop` 换成 `oneshotloop` 即整体替换，默认路径事件序列与默认 loop 逐字节对齐 |

### M3/M4 新增

| 包 | 行数 | 职责 |
|---|---|---|
| `approval` | 153 | 工具审批。监听 `tools/pre-execute`；`mode` 支持 `"auto"` / `"ask"` / 按工具名 map；`Prompter` 接口注入并响应 ctx 取消；拒绝即短路 waterfall，错误进 `tool/result` |
| `tui` | 342 | bubbletea TUI。`render.go` 为纯函数投影（session 事件 → 视图行），可单测；`tui.go` 为薄壳：alt-screen、尾部滚动、y/n 审批抢占键盘 |
| `hxconfig` | 373 | `hx.yml` 组合 + 双层 fiber Stack。新 layer 只能解析本代与 pinned 服务；adapter 支持替换/回滚；`WithLayer` 以读租约覆盖 agent 创建与发布；`ReloadError.Committed` 区分未提交和已提交清理失败；watcher 错误向上上报 |
| `web` | 629 | Web UI：stdlib `net/http` + `go:embed` 单页前端。默认只监听 loopback，远程模式强制显式 opt-in + token；鉴权覆盖全部端点，POST 校验 JSON/Origin/Host；SSE 慢消费者断开、写错误即返回；审批绑定 agent 代际并响应取消 |
| `subagent` | 204 | 子代理委派：独立会话日志、步数预算、深度闸与总超时；等待同时响应调用方 ctx 和 fiber Done，撤销/超时错误携带子 session id 与部分答案 |
| `compaction` | 393 | 上下文压缩 seam。先剪枝后摘要；summary 以 `ThroughSeq` 保留尾部和并发追加事件，滚动蒸馏携带旧摘要；`keep_first_events` 钉住初始目标；prune 幂等且只在摘录更短时落日志，失败重试不重复，工具错误进入摘要；同会话并发共享一次调用 |
| `memory` | 350 | 文件派长期记忆。turn/start 注入持久 user/message，digest 去重、压缩后重注入；读取有 1 MiB 硬上限，`max_bytes` 内保留完整 system-reminder 开闭框架，非法配置 fail loud |
| `toolgate` | 327 | 工具闸门：超阈值只内联 always + tool_search + 已激活工具，搜到即激活且 KV cache 顺序稳定；会话清理由锁外 Effect 登记，首次组装与 Dispose 并发不死锁 |
| `kb` | 1,211 | 知识库 seam。Markdown vault 是事实源，SQLite 索引可弃；增量 Sync 与 watcher 串行避免重复嵌入；`kb_read` 的内容/ETag 同源，`kb_write` 同 ETag 串行校验、临时文件原子替换，索引失败明确报告部分完成；支持混合检索、受控写入、lint 和 librarian prompt |
| `kb/vault` | 152 | Markdown 解析纯函数：frontmatter、wikilink、按 heading 层级分块与行号锚点 |
| `kb/index` | 736 | 嵌入式 SQLite 索引（纯 Go）：`files`/`chunks`/`links`/`embeddings`/`metadata` + FTS5 trigram；单连接保证 WAL/busy_timeout 覆盖；记录 provider/model/维度指纹，变化时从 vault 强制全量重建；单文件事务重写与 FTS/向量混合排序 |
| `kb/embed` | 318 | 嵌入能力：`Embedder` + provider/model 指纹与可选维度声明；OpenAI 兼容 provider 和确定性 `Fake`，响应按输入序对齐 |
| `kb/eval` | 183 | 离线评测 harness：合成 fixture vault（testdata 入库，16 wiki 概念页 + 24 raw 材料笔记，wikilink 成图，lint-clean 由测试钉住）+ 44 题 gold set（自然语言中文提问，单跳/多跳各 22 题，多跳题与目标笔记零关键词重叠、沿双链 2-3 跳，gold.json 自带 Fake 的 Synonyms 词表桥）；真实语料评测 realvault（14 篇真实中文技术文档 596K、706 chunk，语料含第三方内容不随仓库公开、缺失时测试自跳过）+ 28 题 realgold（单跳/多跳各 14，多跳绕开目标文档用词、零 ≥3 字关键词重叠由检查器钉住）；命中率对比报告（纯 FTS vs 混合 1:1 vs 混合 1:0.5，top-1/top-5 分组）+ 搜索/摄入/重建延迟 benchmark；真 API 评测（HX_EMBED_* 指向 OpenAI 兼容端点，无 env 自跳过）是 README 数字的来源口径 |

## 使用方式

```sh
go build -o hx.exe ./cmd/hx

export DEEPSEEK_API_KEY=sk-...

hx "看下当前目录有什么文件并总结"   # 单发任务
hx chat                            # 行模式多轮
hx tui                             # TUI（无参数默认）
hx web                             # Web UI（默认 127.0.0.1:7681）
hx chat --resume <session-id>      # 从日志恢复，上下文逐字节重建
hx tui --config hx.yml --watch     # 配置组合 + 热重挂
```

### hx web

`hx web [--addr 127.0.0.1:7681]` 启动浏览器界面，默认仅本机可访问。非 loopback 监听必须同时指定 `--allow-remote` 和 `--auth-token`（或 `HX_WEB_TOKEN`）；token 覆盖首页、SSE 与全部 POST，POST 还校验 JSON Content-Type、Host 和同源 Origin。SSE 推送 `session`/`approval`/`notice`/`session-changed` 四种信封，慢消费者断开、写错误立即返回。审批绑定 agent 代际，会话切换后旧回答失效，取消或关闭服务器会解除悬挂等待。hub 订阅在 root ctx 上，配置热重挂后事件流不断。

子命令须放在旗标之前（`hx chat --config x.yml`，Go flag 包的标准行为）。

### hx.yml 配置组合

查找顺序：`--config` > `./hx.yml` > `~/.hx/hx.yml`；无配置文件时走内置默认 wire（M2 行为）。格式：

```yaml
pinned: [session, llm]                # 热重挂边界：不随配置重挂重建（默认即这两个，可省略）
plugins:
  - id: session
    name: session
  - id: llm
    name: llm
  - id: deepseek
    name: deepseek
    config:
      api_key: ${DEEPSEEK_API_KEY}
  - id: openrouter                    # 同实现多实例：第二家厂商
    name: deepseek
    config:
      name: openrouter
      base_url: https://openrouter.ai/api/v1
      api_key: ${OPENROUTER_API_KEY}
      model: anthropic/claude-sonnet-4
  - id: tools
    name: tools
  - id: builtin-tools
    name: builtin-tools
  - id: prompt
    name: prompt
  - id: approval
    name: approval
    config:
      mode: { bash: ask, "*": auto }  # bash 逐次询问，其余放行
  - id: agents
    name: agents
  - id: loop
    name: oneshotloop                 # 换 loop 就是改这一行
  - id: subagent
    name: subagent                    # delegate 工具：子 agent 委派
    config:
      max_depth: 1                    # 只有顶层 agent 能委派
      timeout_seconds: 120
  - id: compaction
    name: compaction                  # 上下文压缩：超预算自动摘要
    config:
      max_context_tokens: 16000
      keep_recent_events: 20
      # summary_prompt: ...           # 可选：覆盖默认摘要提示词
  - id: memory
    name: memory                      # 长期记忆：turn/start 时把记忆文件注入会话日志
    # config:                         # 可选：覆盖默认记忆文件位置与预算
    #   user_file: ~/.hx/memory.md    # 用户级（跨项目）
    #   project_file: .hx/memory.md   # 项目级（相对 cwd）
    #   max_bytes: 30720              # 单次注入的字节上限（超限截断+标注）
  - id: toolgate
    name: toolgate                    # 工具闸门：工具数超阈值时裁剪内联集合 + tool_search 按需激活
    # config:                         # 可选：默认值即推荐值；不挂载或 max_inline ≤ 0 时零行为变化
    #   max_inline: 20                # 工具总数 ≤ 此值原样透传
    #   always: [bash, read_file]     # 始终内联的工具名单
    #   search_limit: 5               # tool_search 每次返回的完整 schema 条数
  - id: kb
    name: kb                          # 知识库：vault 索引 + kb_search/kb_read/kb_write
    config:
      vault: ./notes                  # 必填（有 config 时）：Markdown vault 目录
      # index: ./notes/.hx/kb.db      # 默认 <vault>/.hx/kb.db
      # debounce_ms: 200              # watcher 去抖窗口
      # search_limit: 10              # kb_search 默认条数
      # lint_max_chunks: 50           # kb_lint 长文阈值（chunk 数）
      # fts_weight: 1.0               # 混合检索 FTS 路权重（显式 0 关闭该路）
      # vector_weight: 1.0            # 混合检索向量路权重（配了 embedder 且两路都缺省则各取 1）
      # embedder:                     # 可选：配了才走向量嵌入 + 混合检索
      #   base_url: http://localhost:11434/v1  # 缺省回落 HX_EMBED_BASE_URL
      #   api_key: ${HX_EMBED_API_KEY}         # 可空（本地服务）；回落 HX_EMBED_API_KEY
      #   model: bge-m3                        # 缺省回落 HX_EMBED_MODEL；最终缺 base_url/model 即报错
patch:                                # 可选：按 id 整块替换 config（不 merge）
  - id: deepseek
    config: { model: deepseek-reasoner }
```

- `${ENV}` 递归展开；未知字段、缺 name/id、未知名 insert 全部 fail loud；
- `--watch` 时 fsnotify 监听配置文件：变更 → dispose 插件层并以新配置重挂，**会话日志不丢**（pinned 插件不随层重建，名单由 hx.yml 顶层 `pinned` 字段声明，默认 session/llm——不是内核硬编码），在跑的 agent 以同 session id 在新层重建。

### 多厂商 API key

- OpenAI 兼容厂商：如上例，同 `deepseek` 实现换 `base_url`/`api_key`/`model` 多挂实例，`Request.Provider` 按名路由；
- 协议不同的厂商（Anthropic/Gemini 原生）：各写一个 ~200 行 `llm.Adapter` 插件，内核不动。

### kb 评测（kb/eval）

合成 fixture vault（16 wiki 概念页 + 24 raw 材料笔记，wikilink 双链成图，testdata 入库）+ 44 题 gold set（**自然语言中文提问**，单跳/多跳各 22 题；多跳题与目标笔记**零关键词重叠**（无 ≥3 字 CJK 公共子串）、答案沿双链 2-3 跳，gold.json 自带 `embed.Fake` 的 Synonyms 词表桥，模拟真实多语 embedder 的语义迁移）。Fake 三模式离线确定性可复现，真 API 复现只需一个 OpenAI 兼容端点：

```sh
go test -v ./kb/eval/ -run TestGoldSet     # 命中率对比报告（Fake，CI 确定性回归口径）
go test -run=NONE -bench=. ./kb/eval/      # 搜索/摄入/重建延迟
HX_EMBED_BASE_URL=... HX_EMBED_API_KEY=... HX_EMBED_MODEL=... go test -v ./kb/eval/ -run TestRealEmbedder  # 真 API（无 env 自跳过）
```

**真 API 实测**（本机 i5-13600KF，Ollama 本地 bge-m3；纯 FTS 行不经过 embedder，两种口径相同；多跳 top-5 恒 0 是设计性质，由断言钉住）：

| 模式 | 单跳 top-1 | 单跳 top-5 | 多跳 top-1 | 多跳 top-5 | 总计 top-1 | 总计 top-5 |
|---|---|---|---|---|---|---|
| 纯 FTS | 100% (22/22) | 100% | **0% (0/22)** | **0% (0/22)** | 50.0% | 50.0% |
| 混合 1:1（bge-m3） | **100% (22/22)** | 100% | 31.8% (7/22) | **95.5% (21/22)** | 65.9% | **97.7% (43/44)** |

**CI 确定性回归口径**（Fake dim=1024，无外部服务；Fake 的词表桥接是精确 token 对齐，信号远强于真实模型，数字只用于回归断言，不作为能力宣称）：

| 模式 | 单跳 top-1 | 单跳 top-5 | 多跳 top-1 | 多跳 top-5 | 总计 top-1 | 总计 top-5 |
|---|---|---|---|---|---|---|
| 混合 1:1（Fake） | 81.8% (18/22) | 100% | 59.1% (13/22) | 100% | 70.5% | 100% |
| 混合 1:0.5（Fake） | 100% (22/22) | 100% | 54.5% (12/22) | 100% | 77.3% | 100% |

要点：真 API 下混合检索把多跳题从纯 FTS 的 0 救到 top-5 95.5%（唯一 miss 被 Raft 概念 wiki 页截流）；真模型下等权混合不再稀释单跳 top-1（22/22 保持），Fake 口径里的稀释现象（1:1 掉到 18/22、1:0.5 追回 22/22）是 hash 投影噪声的特有产物——两种口径都证明 `fts_weight`/`vector_weight` 可配不是摆设。延迟（fixture 规模 40 篇）：搜索纯 FTS p50 535µs / p95 1.1ms、混合 p50 1.0ms / p95 2.0ms（matchQuery 按滑窗展开词元后 FTS 候选多于旧空格分词口径）；摄入（写入→可检索同步路径，含解析+嵌入+索引事务）p50 5.2ms / p95 9.9ms；全量重建（40 篇）4.0ms。

### 真实语料评测（kb/eval/realvault）

合成 fixture 是控制变量口径；另用 **14 篇真实中文技术文档**（操作系统/Go/MySQL/Redis/MQ/网络/RAG 等八股材料，596K、8,598 行，摄入后 706 chunk）+ **28 题 realgold**（单跳/多跳各 14，多跳题刻意绕开目标文档用词，零 ≥3 字关键词重叠由检查器钉住）做独立交叉验证——回答"gold set 是不是自己出题自己答"。**realvault 语料与 realgold 题集含第三方内容，不随公开仓库分发**；缺失时三个 realvault 测试（FTS-only / 真 embedder / miss 诊断）与 agent 级测试自跳过，本地把语料放回 `kb/eval/testdata/` 即可复现下表数字：

```sh
go test -race -count=1 -v -run "TestRealVaultFTSOnly" ./kb/eval/        # 确定性腿（FTS-only 断言）
HX_EMBED_BASE_URL=... HX_EMBED_API_KEY=... HX_EMBED_MODEL=... \
  go test -race -count=1 -v -run "TestRealVaultRealEmbedder" ./kb/eval/  # 真 API 腿（无 env 自跳过）
```

| 模式 | 单跳 top-1 | 多跳 top-1 | 多跳 top-5 | 总计 top-1 | 总计 top-5 |
|---|---|---|---|---|---|
| 纯 FTS（确定性） | 12/14 | 0/14 | 0/14 | 12/28 | 12/28 |
| 混合 1:1（bge-m3） | **14/14 (100%)** | 5/14 (35.7%) | 7/14 (50.0%) | 19/28 (67.9%) | 21/28 (75.0%) |

多跳 top-5 低于合成 fixture 的 95.5%，如实报告、两口径不混用：真实语料 700+ chunk、同域大文档互相竞争、措辞对抗性更强。摄入：14 文件 → 706 chunk，无 embedder Sync 3.68s、真 bge-m3 Sync 24.8s（含嵌入）。7 道 hybrid miss 经纯向量路（fts_weight=0）诊断**未发现混合截流 bug**：4 题期望笔记裸向量排名就在 10 名外（刻意绕词导致 bge-m3 桥接失败，预期对抗张力）、2 题 rank 9-10（同域竞争）、1 题向量 rank 2 可解但被 FTS 垃圾命中挤出（真实语料下混合权重的已知调优空间）；FTS 侧 2 个单跳 miss 被向量路修复，是 BM25 OR 语义的正常行为。

**miss 的答案级判定**（`realdiag_test.go` 打印混合 top-5 两路分数，逐题看内容能不能用）：7 道 miss 中 3/7 的 top-5 确有能回答问题的 chunk（读首部超时→01§8.2 四超时参数全覆盖；分片键→03§7.4 MySQL 分库分表视角内容同义，文档级 miss 内容级命中；nsq 订阅确认→10§2.3+06§8.8 拼合覆盖），4/7 真 miss——答案级口径多跳 top-5 ≈71%（10/14），高于文档级精确口径的 50%。单文件 expect 是"唯一正确答案"假设，主题稠密语料上过强（BEIR qrels 本来就是人工标注的相关集合）。同域竞争的放大器：17 号速查卡是其余 13 篇的浓缩版，每个主题天然双份。桥接失败的 4 题属 query rewriting 问题域（multi-query/HyDE/decomposition），kb 场景由 agent 迭代检索承担同一职责，但两套 gold 都是单轮口径、迭代收益不在此列。

## 测试体系

- **黄金回放测试**（`agentloop/golden_test.go`）：伪造 SSE 流（文本 + tool_call + 收尾文本）跑完整一轮，断言 14 条 session 事件的 JSONL **逐字节相等**；并验证第二次模型请求等于请求时刻日志的投影（"模型可见 ⟺ 已落日志"的机器断言）；
- **resume 测试**：换 Store 实例重读日志，投影逐字节一致；
- **session 耐久性测试**：`FileLog.Append` 写盘 fsync 后再广播；`readEvents` 遇第一个 JSON 坏行即截断、之前的好事件照常恢复（崩溃写了一半的行及其之后一律不可信），不再整个会话恢复失败；`session/prune` 投影等价性逐事件机器断言（剪枝后模型所见 = 日志投影）；
- **卸载干净测试**：mount → 注册 → Dispose → 断言注册表空、goroutine 数回落（`runtime.NumGoroutine` 前后对比）；
- **替代 loop 测试**：经 hxconfig 真实配置组合挂载 `oneshotloop`，断言 `agents.Create` 返回 oneshot 实现、整段日志逐字节等于黄金串、恰好 2 个请求且第二个不带 tools；
- **热重挂测试**：改配置触发 `Reload`，断言旧插件 disposer 执行过、新配置生效；pinned 边界三种形态（默认 session/llm 存活、显式覆盖、显式空列表全部重建）；pinned 集合变更（顺序无关）拒绝本次重载、错误含新旧名单、旧层保持运行；fsnotify `Watch` 端到端；
- **子代理委派测试**（`subagent/subagent_test.go`）：伪造 SSE 按请求内容路由父/子 agent 的脚本化响应，断言父日志出现 `tool/call`+`tool/result`（内容为子 agent 最终答案）、子会话日志独立完整；深度 1 时子 agent 内调 delegate 被拒；超时路径断言错误结果 + `runtime.NumGoroutine` 回落（子 fiber Dispose 无泄漏）；调用方 fiber Done（热重挂/父 agent 撤销）即时解开返回、不拖到超时；超时与被撤销的错误都带子 session id + 子 agent 部分答案（截断 500 rune）；
- **compaction 测试**（`session` 投影单测 + `compaction/compaction_test.go`）：summary 以 `ThroughSeq` 声明实际覆盖范围并作为 user 消息领投，`keep_recent` 不进 transcript 但保留在投影中，摘要期间并发追加的 user/message 与 tool/result 不丢失；同会话并发调用只请求摘要模型一次、等待者可独立取消且日志只有一条 summary；provider 超预算触发压缩、预算内不动作、tool call/result 对不被边界切开；先剪枝后摘要（超 `prune_bytes` 的 tool/result 以 `session/prune` 落日志、剪完回预算内即跳过 LLM 摘要）；滚动蒸馏携带旧摘要与上一轮保留尾部，多轮压缩不跳过未覆盖事件；`keep_first_events` 钉住会话开头、头部配对保护、二次压缩继承 `FirstKept`，且头部不得越过 `ThroughSeq+1`；全量/增量投影逐事件等价并覆盖保留尾部的分片收尾、prune 重放和冷缓存 resume；集成测试经 agentloop 全栈挂载，断言压缩后请求体仍等于日志投影且被压掉的内容不再出现在请求里；
- **memory 测试**（`memory/memory_test.go`）：turn/start 触发注入，事件落日志且投影为 user 角色可见；digest 未变不重复注入；文件变更后重新注入；summary 之后重新注入（压缩恢复）；`max_bytes` 截断与宽文件省略标注生效；两文件缺失/为空时零注入（日志逐字节只有 turn/start）；config 非法值与路径撞车 fail loud；Dispose 后改文件不再注入；
- **toolgate 测试**（`toolgate/toolgate_test.go`）：工具总数 ≤ `max_inline` 原样透传、超过只内联 `always` + `tool_search` + 本会话已激活；`tool_search` 打分（名称 +2/描述 +1）取 top `search_limit` 返回完整 schema 且**搜到即激活**、后续请求自动携带；顺序稳定性（基础集合名字序、激活工具按激活先后追加尾部、会话内只增不减，KV cache 友好）；会话键取最近 `agent:` 前缀祖先 fiber（无则退化树根）；零监听器与 `max_inline ≤ 0` 空挂载逐字节不变；
- **kb 测试**（`kb/vault` 表驱动 + `kb/index` + `kb/embed` + `kb/kb_test.go`）：frontmatter 缺失/未闭合/损坏、嵌套 heading 栈、别名与锚点 wikilink、CRLF；增量索引幂等（同 sha1 跳过重索引、重复索引不产生重复命中）、重索引替换旧内容、Rebuild 全量可弃、FTS 归一化排序与权重缩放、敌意查询不破 MATCH；混合排序确定性（FTS 路笔记与向量路笔记随权重翻转、显式 0 权重 ≠ 未设置、无查询文本的纯向量检索、余弦 ≤0 不进候选）、重索引重写向量表、向量数与 chunk 数不齐 fail loud、1 万 chunk×256 维暴力扫描 benchmark；OpenAI provider httptest 线格式（路径/Bearer/body、乱序 index 按输入序还原、无 key 不发 Authorization、500 带 body 摘要）、HX_EMBED_* env 兜底与缺字段 fail loud、Fake 确定性/单位范数/同义词余弦≈1、真 API 无 env 自跳过；embedder 配置段端到端（Sync 一次批量嵌入 + 搜索一次查询嵌入，调用计数断言）与非法段 fail loud；`kb_lint` 四类问题（断链/孤立/未索引/长文）各自构造命中、修复后复检、工具层健康文案与拒参；librarian prompt 段后挂载 prompt 也能等到注册（代际等待无错过唤醒）、dispose 后摘除；etag 冲突拒绝且不覆盖文件、写后立即可检索、watcher 去抖后新文件可检索/删除文件被剪除（5 秒上界轮询断言）、config 非法 fail loud、nil config 空挂载、Dispose 回收工具与 watcher goroutine（NumGoroutine 前后对比）；
- **kb/eval 测试**：fixture vault lint-clean 钉住（双链图完整性）；gold set 三模式对比（纯 FTS / 混合 1:1 / 混合 1:0.5）——多跳题纯 FTS top-5 恒 0（零关键词重叠由断言钉住）、混合严格更优、1:0.5 软混合追回单跳 top-1；混合模式连跑两遍命中逐题一致（确定性）；搜索 p95 / 摄入 p50·p95 / 全量重建 benchmark；realvault 真实语料腿——FTS-only 确定性断言（单跳 top-5 ≥10、多跳 top-5 ≤2）+ 真 embedder 双报告（自跳过）+ miss 诊断（`realdiag_test.go` 打印混合 top-5 两路分数，7 道 miss 答案级判定 3/7 能用）；真 API 评测（自跳过）是 README 命中率数字的来源口径；
- 标准验收为 `go build ./...`、`go vet ./...`、`go test -count=1 ./...`；并发、生命周期与共享状态改动还需在 Linux 环境执行 `go test -race -count=1 ./...`（26 个包，24 个有测试）。

## 数字

| 指标 | 值 |
|---|---|
| 总行数 | 18,539（实现 9,271 + 测试 9,268） |
| 直接依赖 | 5 个：`bubbletea`、`lipgloss`、`fsnotify`、`yaml.v3`、`modernc.org/sqlite` |
| 间接依赖 | 24 个（bubbletea/lipgloss 终端链 + modernc.org/sqlite 链） |
| 分发 | 单静态二进制 18.0 MiB（18,905,600 字节，CGO_ENABLED=0），`go build` |
| 对比 dsh | 227 包 / 649 依赖版本 / tsc 双 face + tsdown / Node + tsx + n-API |

## 里程碑完成情况

| 阶段 | 内容 | 状态 |
|---|---|---|
| M1 | fiber + event + plugin | ✅ |
| M2 | session + llm/deepseek + agentloop + 内置工具 + CLI（含 resume 红线） | ✅ |
| M3 | 审批监听器 + prompt 组装 + TUI + llm 命名注册表 | ✅ |
| M4 | hx.yml 组合 + 配置热重挂 + 替代 loop | ✅ |
| M4+ | Web 前端（`hx web`：SSE hub + 内嵌单页 + Web 审批） | ✅ |
| M4+ | 子代理委派（`subagent`：delegate 工具 + 深度闸 + 独立子会话日志） | ✅ |
| M4+ | 上下文压缩（`compaction` seam：summary 水位线事件 + LLM 摘要 Provider + agentloop Consumer） | ✅ |
| M4+ | 长期记忆（`memory`：dsh agent-instructions 形态——记忆文件作为 durable user/message 注入会话日志，digest 去重 + 压缩后重注入 + 字节预算） | ✅ |
| P1 | 知识库（`kb`：Markdown vault + 内嵌 SQLite 索引 + `kb_search`/`kb_read`/`kb_write` 工具，FTS5 trigram 检索、etag 乐观并发写入、fsnotify 增量索引） | ✅ |
| P2 | 向量检索与混合排序（`kb/embed`：OpenAI 兼容嵌入 + 确定性 fake；`embeddings` 表随重索引事务重写；FTS/向量两路各自 min-max 归一化后按权重线性混合重排；权重指针语义区分显式 0 与未设置） | ✅ |
| P3 | 知识库收官（librarian prompt 段：双区约定/wiki 页格式/etag 纪律，代际等待注册随 dispose 摘除；`kb_lint` 确定性体检：断链/孤立/未索引/长文；`kb/eval`：fixture vault + 44 题自然语言 gold set，多跳命中率纯 FTS 0% → 真 API（bge-m3）混合 top-5 95.5%，延迟 benchmark） | ✅ |
| M5 | 加固与规模化（session 耐久性：`FileLog.Append` fsync + 坏行截断恢复 + `session/prune` 事件 + `SummaryData.FirstKept`；compaction 四项：先剪枝后摘要/滚动蒸馏/结构化六段模板/`keep_first_events`；delegate 超时修复：撤销即时解开 + 带子 session id 与部分答案；hxconfig pinned 热改检测：变更即拒绝重载；`toolgate` 工具闸门：`prompt/tools` 瀑布缝 + `tool_search` 搜到即激活 + KV cache 纪律） | ✅ |

## 实现过程中的关键修正（相对原方案）

1. **fiber.Get 全树回退**：插件挂载在子 fiber 上，"只向上解析"使父上下文看不到插件服务。现为祖先链优先（保住子覆盖父语义）、未命中回退全树 DFS。
2. **`iter.Seq[Chunk]` + `Chunk.Err`** 替代 `iter.Seq2[Chunk, error]`：单返回值 range 更顺手。
3. **监听器注册在挂载点 fiber**：事件收集只沿发射方祖先链，插件 fiber 与 agent fiber 是兄弟，挂在自身不可见；disposer 仍绑回插件 fiber，撤销语义不变。
4. **hx.yml 顶层为 `plugins:` + `patch:` 两段**：裸列表无法与可选 patch 共存于 YAML。
5. **fsnotify Windows 后端并发 `Close` 死锁**：watcher goroutine（收到 fiber done）与 disposer 各自调 `w.Close()`，Windows 后端两个并发 Close 互相等不到完成信号（-count=10 压测下表现为 dispose 卡死或 watcher goroutine 残留）；收敛到 `sync.Once` 单点关闭。
6. **服务代际等待的错过唤醒**：librarian prompt 段等 "prompt" 服务时先 `Get` 后读代际，provide 落在两者之间则基线已含新代际、`WaitGeneration` 永远等不到下一次广播；改为先读代际再 `Get`，唤醒必然被其中一侧接住。
7. **向量 top-k 裁剪方向反转**：`insertTop` 维护的是降序（最优在头部），超容量时却 `top[1:]` 砍掉头部——正余弦候选数超过 k 时留下的恰是**分数最低**的 k 个。Fake 下多数 chunk 余弦 ≤0 被候选过滤，正候选恒少于 k（40），bug 潜伏；真 embedder（bge-m3）语义空间里余弦几乎全正，必然触发，向量路整个废掉（真 API 多跳 top-5 曾 0/22，而裸余弦直排期望笔记就在第 2 名）。修复为保留头部 `top[:k]`，并补回归测试（25 条全正余弦 chunk 断言最优不被裁掉）；顺带修了 NumCPU 分片的上取整越界（25 条记录 20  worker 时尾部 worker 的区间为空导致 slice panic）。

## 已知限制与后续方向

- 无进程内热重载（Go 无解），重启加载 + 配置热重挂替代；
- 插件为编译期注册，第三方插件需编入二进制（进程外插件可走未来的 MCP 客户端）;
- 未实现：多 agent 作用域（isolate realm）、沙箱；
- TUI 为功能底线版本，渲染能力可随需要增强（diff 视图、markdown 渲染）；
- 后续方向：MCP 客户端 seam（进程外工具）、按任务类型的多 provider 路由、memory 数据库派 Provider（替代文件派）。

## 血统与许可证

hx 是 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)（MIT License, © DeepSeek）架构脊柱的个人 Go 重写：复刻其验证过的五条设计不变量，未复制其代码。hx 本身以 [MIT 协议](LICENSE)发布。
