# slop-proxy-go

Go 重写版的多账号 LLM 代理，把 ChatGPT/Codex 订阅账号池化到统一的 OpenAI 兼容端点后面。核心池化逻辑参照 Rust 原版 `slop-proxy` 的设计，用 Go 的并发模型实现。

## 设计

请求流：客户端 token → 兼容 API 端点 → 账号池排序 → 上游账号。

池化核心在 `internal/pool/`：

- **Slot 状态机**：每个账号维护凭证、冷却状态、连续失败数、配额窗口、模型冷却表。
- **选号排序**：先按配额档位（Ample/Steady/Behind/Spent，软阈值默认 0.9），再按会话绑定，最后用 rendezvous hash 稳定选择，保证 prompt cache 命中。
- **冷却退避**：429 按 `retry-after` 或指数退避（上限 6 小时），模型级限流只冷却该模型，401 强制刷新一次再失败才冷却，网络错误不惩罚账号。
- **token 刷新串行化**：refresh token 轮换，用每 slot 的互斥锁防止并发刷新互相作废；刷新前和数据库比对，采纳其他进程已轮换的 token。
- **会话粘滞**：session → 账号绑定保留 12 小时，上限 5 万条；首选账号短暂冷却时会先等待。
- **热重载**：每分钟从 SQLite 同步账号表和出站代理文件，登录新账号或更新代理列表无需重启。

## 快速开始

```sh
go build -o slop-proxy-go ./cmd/proxy

# 登录 Codex 账号（设备码流程）
./slop-proxy-go login --provider openai --label my-account

# 其他订阅登录：anthropic (PKCE) / copilot (GitHub device) / glm (z.ai CLI OAuth)
./slop-proxy-go login --provider anthropic --label claude

# 或添加静态 API key
./slop-proxy-go accounts add-key --provider openai --key sk-xxx

# 发行客户端 token
./slop-proxy-go token create --user alice

# 启动服务
./slop-proxy-go serve --bind 127.0.0.1:8484
```

客户端接入：

```sh
curl -H "Authorization: Bearer sp-..." http://localhost:8484/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5","messages":[{"role":"user","content":"hello"}]}'
```

## 配置

默认配置文件 `~/.config/slop-proxy-go/config.toml`，数据库 `~/.local/share/slop-proxy-go/slop.db`。可用 `--config` / `--db` 或 `SLOP_CONFIG` / `SLOP_DB` 覆盖。Go 版可读取 Rust 版 SQLite 中已有的账号和 API token。

```toml
db = "/path/to/slop.db"
bind = "127.0.0.1:8484"
metrics_bind = "127.0.0.1:8485"

[codex]
base_url = "https://chatgpt.com/backend-api/codex"
originator = "codex_cli_rs"
instructions = "You are a helpful coding assistant."
soft_utilization_limit = 0.9
```

## CLI

| 命令 | 说明 |
|------|------|
| `--config PATH` / `--db PATH`（全局） | 指定配置与数据库，热切换时会透传给 successor |
| `login [--provider openai\|anthropic\|copilot\|glm] [--label NAME]` | Codex 设备码 / Anthropic PKCE / Copilot device flow / GLM z.ai CLI OAuth |
| `accounts list` | 列出账号 |
| `accounts add-key --provider P --key K` | 添加静态 API key（Gemini 可用 `--referer`） |
| `accounts import-codex FILE\|-` | 从 `auth.json` 导入 |
| `accounts trust/reserve/egress/users/disable/enable/remove` | 账号管理 |
| `token create --user U [--requests N] [--tokens N] [--window-seconds S]` | 发行带限额的 token |
| `token list/revoke/limits/usage` | token 管理 |
| `serve [--bind ADDR]` | 启动服务，支持 systemd `LISTEN_FDS` 继承 |
| `handoff [--bind ADDR]` | ExecReload 客户端：请求运行中的服务把监听 socket 热切换给新二进制 |
| `stats`、`models` | 统计概览与模型列表 |

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/chat/completions` | OpenAI chat 兼容（流式 + 非流式） |
| POST | `/v1/messages` | Anthropic Messages 兼容 |
| POST | `/v1/messages/count_tokens` | Anthropic route 走真实上游 `/v1/messages/count_tokens` 透传（失败不降级本地估算），其余 route 本地估算 |
| POST | `/v1/responses` | OpenAI Responses 兼容，支持 Codex、Gemini 和对应接口的 Zen 模型 |
| GET | `/v1/responses` | Codex WebSocket 双向中继；无 upgrade 头返回 `426` 触发客户端 POST 回落 |
| POST | `/v1/alpha/search` | Codex web search 透传 |
| GET | `/v1/models` | OpenAI 模型列表，按客户端请求返回 Codex 或 Anthropic 目录格式 |
| GET | `/v1beta/models`、`POST /v1beta/models/{spec}` | Gemini 原生面 |
| GET | `/v1/cache/{session}` | 会话缓存状态 |
| GET | `/config/codex/*`、`/backend-api/*` | 桌面/客户端配置与探测 |
| GET | `/health` | 健康检查（Go 版独有） |
| GET | `/metrics`（独立端口） | Prometheus 指标，35 个 family 无条件输出 HELP/TYPE |
| POST | `/admin/token` | 发行 token，要求管理员密钥 |
| GET | `/admin/tokens` | token 列表，要求管理员密钥 |
| GET | `/admin/accounts` | 账号列表，要求管理员密钥 |

管理接口默认关闭。需要 HTTP 管理接口时，给服务设置 `SLOP_ADMIN_TOKEN`，并用 `Authorization: Bearer <管理员密钥>` 访问。用于推理的 `sp-...` token 无法访问管理接口。命令行管理账号和 token 不依赖这个环境变量。

## 出站代理

`proxy_urls` 可和 `proxy_urls_file` 一起使用。文件每行一个 URL，允许空行和 `#` 注释。使用代理池的请求在配置显式出口后从配置的出口发送。Anthropic 可按账号启用代理池。更新文件会保留未改变的连接，配置错误时继续使用上一次有效列表。

`source_prefixes` 支持 IPv6 前缀，每段使用网络地址后的第一个地址。该地址需要能在运行机器上绑定。`exits` 支持代理地址和本地轮换命令，Zen 的匿名请求遇到出口限流时按出口调度轮换，同一出口的各上游主机共享状态。

## 测试

```sh
gofmt -l . && go vet ./... && go build ./... && go test ./...
```

## 与 Rust 原版的对齐情况

已对齐的主要能力：

- **10 个 provider** 后端与 `pool.Backend` 适配（Codex/Anthropic/Gemini/GLM/DeepSeek/OpenRouter/Radiance/Zen/Copilot/Experiential），含 model pattern 路由、原样转发与 usage/catalog 轮询。
- **协议翻译层**：Chat / Anthropic Messages ↔ Responses IR ↔ Chat SSE / Anthropic SSE，Walker 状态机、反向桥接、签名/思考/工具/图像。
- **池化调度**：配额 band、会话粘滞（12h/5 万条）、冷却与故障转移、token 刷新串行化、service-tier 硬过滤与 trusted 偏好排序、HTTP 200 流内 error 帧早拒（60s / 900s）。
- **Codex WebSocket 全套**：客户端 upgrade → 上游 WS 双向帧中继、101 响应头回显隔离、`x-codex-turn-state` pin 与 900s snooze、逐轮 `AuthToken`/`Admit` 准入、显式 service tier 重拨、`codex.rate_limits` 跨账号平均改写、`error`/`response.failed` 分类与 cooldown、reserved namespace 还原、usage 与失败记账。
- **OAuth 登录**：Codex device flow、Anthropic Claude Code PKCE（remote callback + `code#state` 粘贴）、Copilot GitHub device flow + token mint headers、GLM z.ai CLI OAuth（最终产出静态 `{apiKey}.{secretKey}`）。
- **`/v1/messages/count_tokens`**：Anthropic provider 真实上游透传（含 `RequireClaudeCode` 门禁、model 归一化、上游 header/status/body 透传、pool error → Anthropic error envelope），其余 provider 本地估算。
- **计量与运维**：原版 DB schema 与 usage 全维度（`user/account/provider/requested_model/model/effort/service_tier/dialect`，tool / insight / session / error-by-cause 聚合）、Prometheus 35 个 family 无条件 HELP/TYPE、增量折叠 + 定价重写/账号指纹变化触发全量重建、独立端口 metrics、`stats` 报告、客户端配置面、zstd 请求解压与 192 MiB 上限。
- **verbatim relay 全路径记账**：`/v1/messages`（Anthropic）与 `/v1/responses`（Codex）的直接透传路径都写完整 `UsageRecord` 行 —— SSE/JSON 双形态 usage 扫描、cache/reasoning/tool/insight/session 维度、成功 / 非 2xx / pool 失败 / 流中断各出口、零 token 的失败行也入库；并补齐 `RequireClaudeCode` 门禁（403 + `not_claude_code` 记账）、`/v1/messages` 的上游响应头白名单（`copyRelayHeaders`，过滤 `anthropic-ratelimit-*`）、Anthropic 的 `anthropic-ratelimit-unified-*` 与 Codex 的 `x-codex-primary/secondary-*` pool 平均限流头。Codex Responses 流式响应保留 safety 相关响应头及代理计算的池限额。Rust 的 `alpha/search` 与 `backend_get` 本身不记账，Go 同样不记。
- **零停机热切换（handoff）**：`LISTEN_FDS` 继承（fd 3 起、api + metrics 双 socket）、`$RUNTIME_DIRECTORY/control` Unix control socket（NUL argv + 写半关闭 + `ok`/错误串回复）、successor spawn（fd 3+ 并移除 `LISTEN_PID`）、`MAINPID+READY=1` sd_notify、主动 SIGTERM 前任、2 分钟 READY_LIMIT、15 分钟 drain、连续 handoff 可复用 control socket。
- **CLI**：全命令与完整 `LimitArgs`，新增 `--db` 全局参数；`accounts trust/reserve/egress/users/disable` 支持 clap 风格的 `ID --off` 参数顺序。

## 平台与兼容范围

Windows 可以原生构建和启动 HTTP 服务。systemd 监听 socket 继承和零停机热切换在 Unix 部署路径上使用。

兼容性测试使用 SQLite 测试数据库和模拟上游，覆盖旧库读取、管理接口密钥、限额、流式输出、模型目录和 Gemini 协议转换。实际可用模型、账户限额和登录流程取决于相应服务。

Go 版提供 `/health`。会话用量的 `turn_index` 以同一会话的请求序号统计，Rust 原版按请求体里的消息或输入条数统计。
