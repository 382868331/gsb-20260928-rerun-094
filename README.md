# 后台作业执行平台

Go 实现的本地后台作业原型，包含作业定义、运行实例、持久化、本地 worker、失败重试、取消和重启恢复。

## 构建与运行

```bash
go build ./cmd/jobplatform
go test ./...

./jobplatform submit -store jobs.json -type echo -retries 1 -args '{"text":"hello"}'
./jobplatform worker -store jobs.json -once
./jobplatform list -store jobs.json
./jobplatform get -store jobs.json job-000001
./jobplatform cancel -store jobs.json job-000001
```

首轮只注册项目自带的短任务：

- `noop`：立即成功，可安全重试。
- `echo`：返回参数 `text`，纯计算且无外部副作用，可安全重试。
- `fail`：立即返回失败，可安全重试。

新增作业类型通过实现 `jobplatform.Task` 并注册到 `Registry` 完成。任务类型不能修改作业状态，只负责执行业务逻辑并声明本次尝试是否可安全重试。这样新增类型不会改变状态流转。

## 状态与重试

作业状态为 `queued`、`running`、`succeeded`、`failed`、`canceled`、`ambiguous`。每次执行都会在同一作业下追加一条 attempt；`active_attempt` 标识当前代次。安全任务失败且 attempt 编号不超过 `max_retries` 时，作业回到 `queued`，下一次派发生成新的 attempt；不安全任务失败不会自动重试。

确认结果时必须同时匹配作业 ID 和 attempt 编号。旧尝试迟到的成功或失败会得到 `ErrStaleAttempt`，不能覆盖当前 attempt 或作业最终结果。

排队作业收到取消后立即进入 `canceled`，不会再被领取。运行中的作业取消会传播 `context.Context`；如果业务任务尊重取消，当前 attempt 结束后作业进入 `canceled`，不会因失败而重新排队。若任务无视取消并成功，作业仍遵循取消请求，结果不会作为成功发布。

## 重启与重复风险

状态原子写入 JSON 临时文件后 rename。worker 启动时先执行恢复：

- `queued`：继续派发；已请求取消的改为 `canceled`。
- `succeeded`、`failed`、`canceled`：保持结果不变。
- 上次崩溃留下的 `running`：attempt 标记为 `interrupted`。
- 中断的安全任务且仍有重试额度：回到 `queued`。
- 中断的不安全任务或已无重试额度：进入 `ambiguous`，等待人工确认。

“执行成功但确认前中断”存在 at-least-once 重复风险，平台无法从外部判断副作用是否已经发生。因此只有纯计算、幂等写入或任务自身使用幂等键保证重复执行无害时，`RetrySafety` 才能返回 `RetrySafe`。外部支付、发消息、非幂等文件修改等任务应返回 `RetryUnsafe`，重启后会被隔离到 `ambiguous`，而不是自动重试。
