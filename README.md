# 后台作业执行平台

Go 实现的本地后台作业平台，包含作业定义、运行实例、持久化状态机和本地轮询工作者。首轮只注册项目内置短任务：`echo`、`flaky`、`sleep`、`unsafe_flaky`、`unsafe_payment`。

## 状态流转

- 作业：`PENDING -> RUNNING -> SUCCEEDED`，失败且可安全重试时回到 `PENDING`，否则进入 `FAILED`。
- 依赖未满足的作业保持 `PENDING`；前置确认失败或被取消时，后继进入 `SKIPPED`，不会派发。
- `PENDING` 或 `RUNNING` 都可进入 `CANCELED`；终态作业不能再次取消。
- 每次派发都会创建独立尝试记录；重试是同一作业的第 2 次或后续尝试。
- 完成结果只在 `(job ID, current attempt)` 匹配时提交，旧尝试的迟到成功或失败会被拒绝为 `stale attempt`。

## 作业依赖与结果接续

- `enqueue --dep <job-id>`（可重复）声明前置；只有全部前置 `SUCCEEDED` 后作业才会被派发。
- 派发时前置的确认结果通过 `Input.Deps`（前置 ID -> 结果）传给后继，内置 `collect` 类型演示了产物汇总。
- 非幂等任务失败或确认前崩溃时结果不确定：作业进入 `FAILED` 且标记 `needs_verification`，后继保持阻塞，既不派发也不跳过。
- 运维核实外部结果后用 `verify <job-id> succeeded|failed [note]` 裁决：确认成功则后继用核实结果继续；确认失败则后继 `SKIPPED`。不允许直接按失败重做可能有外部影响的步骤。
- 重试只是同一作业的新尝试（`attempt_count` 递增），不会产生新的业务任务；重启后依赖关系图原样恢复。

## 重试边界

平台提供的是“至少一次”执行语义：如果任务已产生外部副作用，但工作者在确认前崩溃，恢复后无法判断副作用是否完成。

- `SafeToRetry: true` 只应标记幂等、可去重或无外部副作用的任务；崩溃恢复会重新派发。
- `SafeToRetry: false` 的普通失败不重试；确认前崩溃会直接标记 `FAILED`，避免对可能已执行的外部操作重复扣费或重复提交。
- 新增作业类型只需要注册 `Definition`，不需要修改状态流转。

## 持久化与取消

状态默认写入 `data/jobs.json`，每次事务通过同目录锁文件串行化，并以临时文件原子替换。工作者启动时会把上次遗留的 `RUNNING` 尝试标记为 `INTERRUPTED`，再按任务的安全重试策略恢复；已经完成的作业和结果保留不变。

取消命令会先把作业原子切到 `CANCELED`。待执行作业因此不会再被派发；运行中的任务通过 context 收到取消信号，工作者之后也不会让旧尝试覆盖已取消状态。

`cancel-group <group-id>` 取消同组（`enqueue --group`）所有未终态作业：已完成的记录原样保留，运行中的尝试获得独立的 `CANCELED` 终态，组外作业不受影响。

## 使用

```bash
go run ./cmd/jobctl types
go run ./cmd/jobctl enqueue echo hello
go run ./cmd/jobctl enqueue flaky demo 3
go run ./cmd/jobctl enqueue --dep <job-id> --group batch collect
timeout 2s go run ./cmd/jobctl worker
go run ./cmd/jobctl list
go run ./cmd/jobctl show <job-id>
go run ./cmd/jobctl cancel <job-id>
go run ./cmd/jobctl cancel-group <group-id>
go run ./cmd/jobctl verify <job-id> succeeded "charged: order-1"
go run ./cmd/jobctl recover
```

`worker` 默认持续轮询；没有待执行任务时也会保持运行。重试回退间隔当前为 100ms，便于内置短任务快速完成。

## 验证

```bash
go test ./...
go test -race ./...
```
