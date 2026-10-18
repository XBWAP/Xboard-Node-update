# 变更记录：用户增量（add/remove）生效问题修复

## 一、问题现象

面板下发 `sync.user.delta` 后，被移除的用户（流量超额/过期）**依然可以继续使用节点**，
只有手动 `systemctl restart xboard-node` 才生效。

```
ws recv event=sync.user.delta data={"action":"remove","users":[{"id":5}],"node_id":9}
```

## 二、根因分析（四个层次，逐层排查得出）

### 1. 所谓"热重启"其实只是热替换 inbound 用户表

`remove` 事件原本走 `kernel.RemoveUsers()` → `reloadInboundsLocked()`，
内部调用 sing-box 的 `adapter.UpdatableInbound.UpdateUsers()` 替换 inbound 里的用户数组。

- 效果：**新的连接**因凭证不存在被拒绝
- 缺失：不关闭**已经建立的连接**，已连接的 TCP/UDP、mux、Hysteria/QUIC 会话继续被转发

这是"用户依旧能用"的直接原因。

### 2. UUID 补全不是解药

曾尝试在 delta 缺少 `uuid` 时从本地缓存回填（`backfillUUIDs`）。
UUID 在项目中只用于三件事：凭证本身（认证新连接）、流量记账（`uuidMap` 映射回 userID）、
限速/设备限制查找。**它与"关闭已有连接"无关**，所以补全 UUID 后现象不变。

（该方案已按需求移除，见第四节。）

### 3. 踢下线的能力本身就是空的

- `ConnTracker.CloseByUUID()` 是空实现，直接 `return 0`
- `kernel.CloseUserConnections()` 全代码库**零调用点**
- `connMap` 只有 `connID → conn`，没有 UUID/用户维度索引，UDP 连接更是完全没记录

### 4. machine 模式下 delta 根本到不了处理逻辑（本次真正的卡点）

生产环境运行的是 **machine 模式**（日志可见 `POST /api/v2/server/machine/nodes`）。
该模式下 delta 的走向是：

```
WS delta → Orchestrator.onWSEvent → mailbox.Apply → drainMachineMailbox
        → handleWSEvent(EventSyncUsers) → applyUserUpdate（热更新，从不重启）
```

`applyUserDelta`（原本写重启逻辑的地方）**永远不会被调用**。

而且当 mailbox 没有基线（`hasFullUserState == false`）时，delta 会被降级成
"需要重新对账"：

```
machine mailbox drained config=false users=false devices=false reconcile=true
ws state may be stale, scheduling REST reconciliation
GET /api/v2/server/user → 304 Not Modified
```

REST 轮询走 ETag 缓存，面板回 304，节点认为"没变化"，**变更被彻底吞掉，且不会自愈**。

## 三、最终方案

**收到任何用户增量（add/remove）→ 重启 xboard-node 服务。**

重启之所以能根治：
- 关闭内核 → `box.Close()` 关闭所有 socket，被移除用户的连接被强制断开
- 全新进程 = 全新 API client = **ETag 缓存清空** → 拉到全量用户列表 → 状态必定收敛

采用"进程内重启"而非自杀进程：复用 `main.go` 已有的配置热重载通道（停止全部服务 →
用同一份配置重新启动），不依赖 systemd，Windows/裸进程下同样成立。

## 四、改动清单

### `internal/service/service.go`

| 改动 | 说明 |
|---|---|
| 移除 `backfillUUIDs` 及 add/remove 两处调用 | UUID 补全方案废弃 |
| 新增 `restartHook` + `SetRestartHook()` / `requestRestart()` | 向宿主进程请求重启；返回 false 表示未接线 |
| `requestRestart(action, reason)` 内输出日志 | 触发前打印 `service will restart`，带 action 与 reason |
| `applyUserDelta` 的 `add` 分支 | 合并后 hash 有变化 → 请求重启 |
| `applyUserDelta` 的 `remove` 分支 | 本地确实存在被删用户（`removed > 0`）→ 请求重启 |
| `applyUserUpdate` | 用户集发生增删且非首次启动 → 请求重启（覆盖 machine 模式 mailbox、单节点 `sync.users`、REST 轮询三条路径） |
| 新增 `diffUsers()` / `countNewUsers()` | 计算增删数量，用于 reason 与防抖判断 |
| 新增 `logDeltaEvent()` / `logUsersSetSkipped()` | 把原本静默的跳过路径变成可见日志 |
| `drainMachineMailbox` | 新增 `machine mailbox drained` Debug 日志 |

### `internal/machine/machine.go`

| 改动 | 说明 |
|---|---|
| 新增 `restartHook` + `SetRestartHook()` | 透传给每个节点 Service，reason 带 node_id 前缀 |
| `startNode` | 给每个 Service 安装重启钩子 |
| `onWSEvent` | **拦截 `sync.user.delta`，直接请求重启**，不再降级为 reconcile |

### `cmd/xboard-node/main.go`

| 改动 | 说明 |
|---|---|
| 新增 `restartCh` / `restartReasonCh` / `onRestart` | 每个 service 实例装配重启钩子 |
| `select` 新增 `case <-restartCh` | `cancel()` → `<-doneCh` → 用当前配置重新启动全部服务 |
| 退出判断改为 `newRoot == nil && !restartRequested` | 避免把重启误判成正常退出 |
| 新增 `restart complete, services restarting with current config` | 重启完成日志 |

## 五、防抖与兜底

- **防抖**：只在状态真的变化时重启
  - `remove`：`removed > 0`（本地确实有该用户）
  - `add`：`computeUserHash(merged) != lastUserHash`
  - `applyUserUpdate`：`len(lastUsers) > 0` 且确有增删（首次启动不重启，避免启动即重启的抖动）
- **兜底**：重启钩子未接线时（单测、嵌入式调用）退回原有的热更新路径，不会出现"什么都不做"
- **流量不丢**：重启前主动 `trackAndEnforce(ctx)` 收割内核字节计数（重启会重置计数器），
  `Run()` 在 `ctx.Done()` 时还会执行一次 `pushReportSync()`

## 六、诊断日志

| 日志 | 用途 |
|---|---|
| `machine mailbox drained config= users= devices= reconcile=` | 确认 delta 有没有变成用户表变更 |
| `user set unchanged, skipping (incoming=N local=M ids=...)` | 确认是否因 hash 未变而静默跳过，会打印本地用户 ID |
| `user delta ignored action= ids= why=` | 单节点模式 delta 被跳过的原因 |
| `service will restart action= reason=` | 重启触发点（Debug，触发前） |
| `requesting service restart action= reason=` | 已向宿主发出重启请求（Warn） |
| `machine: user delta received, requesting service restart node_id= action= users=` | machine 模式入口拦截 |
| `service restart requested, restarting all services... reason=` | main 已响应重启 |

## 七、部署后应看到的完整链路

```
[core] ws recv event=sync.user.delta data={"action":"remove","users":[{"id":5}],"node_id":9}
[core] ws sync user delta event received
[core] machine: user delta received, requesting service restart node_id=9 action=remove users=1
[core] service restart requested, restarting all services... reason="node 9: user delta remove (1 users)"
[core] restart complete, services restarting with current config
```

单节点模式：

```
[core] ws sync user delta event received
[vless:443] users delta: remove, 1 users
[vless:443] service will restart action=remove reason="user removal: -1"
[vless:443] requesting service restart action=remove reason="user removal: -1"
[core] service restart requested, restarting all services... reason="user removal: -1"
```

## 八、验证

- `go build ./internal/... ./cmd/...` 通过
- `go vet ./internal/service/ ./internal/machine/ ./cmd/...` 无告警
- `go test ./internal/service/ ./internal/controlplane/ ./internal/panel/ -count=1` 通过
- 生产环境实测生效

新增用例：

| 用例 | 断言 |
|---|---|
| `TestApplyUserDeltaRemoveRequestsServiceRestart` | remove 触发重启，不调用 `Start`，内核保持运行 |
| `TestApplyUserDeltaRemoveUnknownUserDoesNotRestart` | 本地不存在的用户不触发重启 |
| `TestApplyUserDeltaRemoveLastUserRequestsServiceRestart` | 删掉最后一个用户仍请求重启 |
| `TestApplyUserDeltaRemoveFallsBackToHotSwapWithoutHook` | 无钩子时退回热更新 |
| `TestApplyUserDeltaAddRequestsServiceRestart` | add 触发重启，不调用 `AddUsers` |
| `TestApplyUserDeltaAddNoChangeDoesNotRestart` | 无变化的 add 不重启 |
| `TestApplyUserUpdateRequestsServiceRestartOnRemoval` | machine 模式路径（全量同步）移除触发重启 |
| `TestApplyUserUpdateNoChangeDoesNotRestart` | 用户集未变不重启 |
| `TestApplyUserUpdateFirstUsersDoesNotRestart` | 首次用户不重启（防启动抖动） |

## 九、已知副作用

1. 重启会断开**该机器（machine 模式）上所有节点**的全部连接，不只是被变更的用户。
2. 旧实例最长排空 5s 才硬关闭（`drainTimeout`），被踢用户最长还有约 5s 可用窗口。
3. 在线设备数（alive IP）在重启瞬间清零，下一个上报周期恢复。
4. 若面板频繁重复推送同一 delta，会造成频繁重启；届时可在 `onWSEvent` 的拦截处
   增加去重（mailbox 有基线时先比对是否真带来变化）。
