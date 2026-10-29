# AGENTS.md

hx 是 DeepSeek Harness 架构脊柱的个人 Go 重写：插件化 agent 运行时，目标是单静态二进制、少量直接依赖、可恢复会话与可逆插件生命周期。修改本目录前，先读 README 了解当前架构与功能口径。

## 开发规范

### 架构不变量

- 一切插件副作用经 `Effect` 登记并返回 disposer；卸载必须逆序、幂等、级联清理，不留 goroutine、监听器或注册项。
- 新能力优先做成插件或挂到已有 seam，不直接修改 agent loop；必须修改 loop 时，同时更新 README 的架构说明。
- 配置非法值 fail loud，在挂载或最早可解析点报错，不静默跳过、不静默降级。
- 模型可见内容必须先写会话日志、成功后再广播；summary/prune 只改变模型视图，不删除原始事件。
- `DeriveMessages` 是全量投影定义，增量 fold 必须与其逐事件等价；修改 session 事件或投影语义时，两处必须一起改并补差分测试。
- 瀑布监听器按约定调用 `next()`；短路与改写必须是有意行为。
- 取消与生命周期不能只覆盖 HTTP：新代码涉及工具、子进程、后台任务时，必须明确 ctx、fiber Done、Dispose 的退出路径。
- 持有插件内部锁时，不调用可能同步执行外部回调或 disposer 的 API，避免自死锁。

### 代码风格

- 注释使用中文，陈述完整契约和边界；不写复述代码、显而易见的注释。
- 保持现有包结构与命名，不为抽象而抽象；跨包边界显式解析依赖，不隐藏默认行为。
- 默认 `CGO_ENABLED=0`，保持静态单二进制。新增直接依赖必须先说明它删除的自有代码和必要性，避免引入 HTTP/LLM SDK 等已有意手写的层面。
- 错误必须包含可定位的上下文；不能用空 catch、静默忽略或「成功返回 + 空结果」掩盖失败。
- 测试与实现同包放置，优先表驱动；并发资源用 `runtime.NumGoroutine` 前后对比辅助发现泄漏。

### 测试要求

- 行为改动必须带对应测试；bug 修复先让测试能覆盖原触发路径，再修实现。
- 不只测正常返回：故障后要验证会话能否继续第二轮、资源是否回收、磁盘与内存是否一致。
- 以下路径按需补故障注入：异常 EOF、流中取消、超时、并发 Dispose、热重挂、坏行、写盘/fsync 失败、重复调用。
- session 投影需覆盖全量/增量等价、resume、fork、summary/prune 组合与逐前缀检查。
- 黄金回放保持逐字节稳定；无监听器、空挂载、缺省配置不得意外改变既有行为。

## 验收命令

每次提交前至少运行：

```sh
go build ./...
go vet ./...
go test -count=1 ./...
```

涉及并发、生命周期或共享状态的改动，还必须在有 CGO/gcc 的 Linux 环境运行：

```sh
go test -race -count=1 ./...
```

Linux 测试机固定为 `192.168.1.4`，SSH 使用本机已配置的 `flywhitelaptop` 别名与对应密钥。Windows 本机只运行上述快速检查，不执行 Docker 或伪造 race 结果；通过 SSH 在 Linux 临时目录检出待验收提交，并在 Linux Docker 中执行：

```sh
docker run --rm \
  -e GOPROXY=https://goproxy.cn,https://mirrors.aliyun.com/goproxy/,direct \
  -v "$HOME/.cache/go-mod:/go/pkg/mod" \
  -v "$PWD":/src -w /src golang:1.27-bookworm \
  sh -c "git config --global --add safe.directory /src && go mod download && go build ./... && go vet ./... && go test -race -count=1 ./..."
```

改变发布行为后，还需：

- 重新执行 `CGO_ENABLED=0 go build -o hx.exe ./cmd/hx`，实测二进制大小。
- 更新 README 中受影响的功能、配置和测试口径；行数、包数、依赖数、二进制大小变化时必须同步，不保留旧数字。
- 关联设计文档有对应描述的，按同一事实源一起更新。

## 提交规范

### 提交前检查

- 工作区只包含本次任务相关改动；不混入格式化、无关重构或多个独立行为变更。
- 代码、测试和对应文档在同一个提交中完成。
- 验收命令已实际运行并通过；未运行 race 时，在提交说明或汇报中明确说明，不写「race 全绿」。
- 不提交 API key、`.env`、真实凭据、个人语料、`.cache/`、`hx.exe`、`*.test.exe` 等产物。
- `kb/eval/testdata/realvault/` 与 `realgold.json` 不随仓库分发；不得绕过 `.gitignore` 强制加入。

### Commit message

使用 `<type>: <中文说明>`，类型包括：

- `feat`：新能力或新的可观察行为
- `fix`：缺陷修复
- `docs`：仅文档变更
- `style`：不影响行为的格式调整
- `refactor`：不改变外部行为的重构
- `test`：仅测试变更
- `chore`：构建、工具或杂项维护

示例：

```text
fix: 修复压缩后 keepRecent 事件退出模型投影

SummaryData 增加 ThroughSeq，摘要只覆盖实际送入模型的事件；
补摘要期间并发 Append 与多次压缩的回归测试。
```

标题说明「改了什么」，正文按需说明「为什么、如何改、如何验证」；不要只写 `update`、`fix bug`、`修改代码`。

### 分支与推送

- main 必须始终可构建、测试可通过。
- 独立改动拆成独立提交；大改动优先按协议、实现、测试拆成可审查的小步。
- 禁止对 main 使用 force push；需要改写未推送的本地提交时，使用 amend 并确认尚未公开。
- 不把临时调试文件、复现脚本、性能产物或个人环境配置提交进仓库。
- commit/push 前确认变更范围；未经用户要求，不擅自发布 release、改仓库可见性或操作 GitHub 设置。
