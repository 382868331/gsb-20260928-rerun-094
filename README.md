# 后台作业执行平台

Go 实现的本地后台作业平台，包含作业定义、运行实例、持久化状态机和本地轮询工作者。首轮只注册项目内置短任务：`echo`、`flaky`、`sleep`、`unsafe_flaky`、`unsafe_payment`。

## 状态流转

- 作业：`PENDING -> RUNNING -> SUCCEEDED`，失败且可安全重试时回到 `PENDING`，否则进入 `FAILED`。
- `PENDING` 或 `RUNNING` 都可进入 `CANCELED`；终态作业不能再次取消。
- 每次派发都会创建独立尝试记录；重试是同一作业的第 2 次或后续尝试。
- 完成结果只在 `(job ID, current attempt)` 匹配时提交，旧尝试的迟到成功或失败会被拒绝为 `stale attempt`。

## 重试边界

平台提供的是“至少一次”执行语义：如果任务已产生外部副作用，但工作者在确认前崩溃，恢复后无法判断副作用是否完成。

- `SafeToRetry: true` 只应标记幂等、可去重或无外部副作用的任务；崩溃恢复会重新派发。
- `SafeToRetry: false` 的普通失败不重试；确认前崩溃会直接标记 `FAILED`，避免对可能已执行的外部操作重复扣费或重复提交。
- 新增作业类型只需要注册 `Definition`，不需要修改状态流转。

## 持久化与取消

状态默认写入 `data/jobs.json`，每次事务通过同目录锁文件串行化，并以临时文件原子替换。工作者启动时会把上次遗留的 `RUNNING` 尝试标记为 `INTERRUPTED`，再按任务的安全重试策略恢复；已经完成的作业和结果保留不变。

取消命令会先把作业原子切到 `CANCELED`。待执行作业因此不会再被派发；运行中的任务通过 context 收到取消信号，工作者之后也不会让旧尝试覆盖已取消状态。

## 使用

```bash
go run ./cmd/jobctl types
go run ./cmd/jobctl enqueue echo hello
go run ./cmd/jobctl enqueue flaky demo 3
timeout 2s go run ./cmd/jobctl worker
go run ./cmd/jobctl list
go run ./cmd/jobctl show <job-id>
go run ./cmd/jobctl cancel <job-id>
go run ./cmd/jobctl recover
```

`worker` 默认持续轮询；没有待执行任务时也会保持运行。重试回退间隔当前为 100ms，便于内置短任务快速完成。

## 验证

```bash
go test ./...
go test -race ./...
```
