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

## 作业依赖与结果接续

`enqueue --depends-on id1,id2` 声明前置作业，构成有向无环图（依赖必须在提交时已存在，因此天然无环）。

- 后继作业只有在全部前置 `SUCCEEDED` 后才会被派发；派发时把已确认成功的前置结果以 `Input.DepResults`（前置作业 ID -> result）交给任务，实现部分结果接续。
- 任一前置 `FAILED` 或 `CANCELED` 时，后继直接转为 `CANCELED` 并记录原因，不会消费未确认的产物。
- 前置因工作者失联而结果不确定（`SafeToRetry: false` 被 `recover` 标记失败）时，后继同样被阻断：必须先人工核实外部副作用，平台不会按普通失败自动重做可能有外部影响的步骤。
- 前置重试只是同一作业的第 N 次尝试，尝试记录挂在原作业下，不会生成新的业务作业；依赖始终引用作业 ID，重启后恢复同一张关系图。

## 分组取消

`enqueue --group 名称` 把作业归入一组，`cancel-group <名称>` 取消整组：

- 已经完成的作业（`SUCCEEDED`/`FAILED`）记录与结果原样保留。
- 仍在执行的任务获得独立终态：当前尝试标记为 `CANCELED` 并写入完成时间，迟到结果依旧被拒绝。
- 组外作业不受影响；组内后继若因前置被取消而阻断，同样按依赖失败规则终止。

## 产物修订

已经 `SUCCEEDED` 的上游作业可以提交修订候选，声明替换或撤销哪些产物：

- `revise --replace <job>=<新结果> [--replace ...] [--withdraw <job>]` 创建修订候选并返回影响分析：哪些下游作业会被重做（`affected`）、哪些已完成结果仍可沿用（`reusable`）、哪些分支卡在未核实的外部效果上（`pending_external`）。
- `revise-confirm <rev-id> [--only id1,id2]` 确认整组修订，或只接受部分产物变化；其余变化保持待确认，可稍后再次 confirm。`revise-cancel <rev-id>` 放弃尚未确认的变化，已确认的部分保留。
- 每次确认都会为目标产物发布新的**代（generation）**并追加到 `result_history`；原有尝试记录与历史代原样保留，修订不会把历史执行改写成从未发生。`revise-show` 可比较每个变化修订前后的结果与代。
- 受影响的后继作业被重置为 `PENDING` 并记录触发修订的 `revision_id`；重新派发时尝试记录 `dep_generations`，明确引用本次确认的产物代，避免混用不同代的候选结果。两次修订交错时，下游只按最新确认的一代重跑一次。
- 撤销（withdraw）产物后，依赖它的作业按依赖失败规则阻断，也不允许再以它为前置提交新作业。
- 修订波及 `SafeToRetry: false` 的作业时，平台不会自动重做可能已产生外部副作用的步骤，而是把它单独列为待决事项；`resolve <job-id> confirmed <结果>` 表示外部效果确实发生（结果作为人工确认的代发布，分支继续），`resolve <job-id> failed` 表示未发生（分支按失败终止）。崩溃恢复产生的不确定失败同样通过 `resolve` 给出结论后才会继续相关分支。

## 持久化与取消

状态默认写入 `data/jobs.json`，每次事务通过同目录锁文件串行化，并以临时文件原子替换。工作者启动时会把上次遗留的 `RUNNING` 尝试标记为 `INTERRUPTED`，再按任务的安全重试策略恢复；已经完成的作业和结果保留不变。

取消命令会先把作业原子切到 `CANCELED`。待执行作业因此不会再被派发；运行中的任务通过 context 收到取消信号，工作者之后也不会让旧尝试覆盖已取消状态。

## 使用

```bash
go run ./cmd/jobctl types
go run ./cmd/jobctl enqueue echo hello
go run ./cmd/jobctl enqueue flaky demo 3
go run ./cmd/jobctl enqueue --depends-on <job-id> --group pipeline echo next
timeout 2s go run ./cmd/jobctl worker
go run ./cmd/jobctl list
go run ./cmd/jobctl show <job-id>
go run ./cmd/jobctl cancel <job-id>
go run ./cmd/jobctl cancel-group pipeline
go run ./cmd/jobctl revise --replace <job-id>=new-result
go run ./cmd/jobctl revise-confirm <revision-id>
go run ./cmd/jobctl revise-show <revision-id>
go run ./cmd/jobctl resolve <job-id> confirmed "verified result"
go run ./cmd/jobctl recover
```

`worker` 默认持续轮询；没有待执行任务时也会保持运行。重试回退间隔当前为 100ms，便于内置短任务快速完成。

## 验证

```bash
go test ./...
go test -race ./...
```
