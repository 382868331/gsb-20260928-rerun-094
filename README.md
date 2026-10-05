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

## 持久化与取消

状态默认写入 `data/jobs.json`，每次事务通过同目录锁文件串行化，并以临时文件原子替换。工作者启动时会把上次遗留的 `RUNNING` 尝试标记为 `INTERRUPTED`，再按任务的安全重试策略恢复；已经完成的作业和结果保留不变。

取消命令会先把作业原子切到 `CANCELED`。待执行作业因此不会再被派发；运行中的任务通过 context 收到取消信号，工作者之后也不会让旧尝试覆盖已取消状态。

## 产物修订与版本化接续

`propose-revision` 只保存候选变化，不会改写已经完成的作业。候选支持一次包含多个变化，也支持只确认其中一部分：

- `preview-revision` 只读计算影响：使用被改代际的已完成下游会重做，撤销会取消下游，未消费该代际的产物保持可复用。
- `confirm-revision --indexes 0,2` 只确认选定变化；未选择的变化仍是 `PROPOSED`，不会进入任何下游输入。
- `cancel-revision` 可取消整组，或用 `--indexes` 取消尚未确认的部分变化；历代 artifact 和已完成 attempt 均保留，不删除或覆盖历史。
- 确认替换会把旧 artifact 标记为 `SUPERSEDED` 并新增一代；重跑 attempt 记录 `DepGenerations`，只读取当时已确认的 active artifact。
- 两次候选交错时，候选固定 `from_generation`；旧候选在新生代上确认会返回 `ErrStaleGeneration`。多个已确认修订若都使同一下游失效，该下游只产生一次新 attempt，attempt 会记录涉及的所有修订 ID。
- `compare-revision` 比较候选记录与当前 artifact 谱系，列出修订前结果、确认结果、当前结果以及是否真的产生变化。

候选可以携带尚未核实的外部效果。确认后对应作业保存 open pending item 并标记 external pending，相关下游保持等待；保存重开后状态仍在。人工核实后使用 `resolve-pending` 标记为 `CONFIRMED` 或 `DISMISSED`，相关分支才会继续派发。

## 审核责任分工

候选中的每个变化都可以用 `Reviewer` 指派给一位审核者，同一候选可由不同审核者分别处理：

- `review-revision --reviewer 名称 --decisions 0:ACCEPT,1:RETURN` 一次提交一个或多个决定。只有变化的指派审核者能决定它；其他人的提交会被拒绝（报告 `denied_indexes`），拒绝事件仍写入该变化的 `reviews`，但不会改变状态、不会提升产物代号，也不会进入任何下游输入。
- 实际生效的决定记录在变化的 `status`、`decided_by`、`decided_at`；每次提交（包括越权、重复）都追加到 `reviews`，其中 `authorized`/`applied` 标明是否获准、是否真正生效。
- `ACCEPT` 交叉确认候选并执行既有确认语义（升代、失效下游或撤销）；`RETURN` 退回候选（状态 `REJECTED`），其待决外部事项一并 `DISMISSED`，候选产物永不进入下游。
- 同一审核操作重复提交是幂等的：与已生效决定相同的提交记为 `duplicate_indexes`，不重复提升产物代号；与已生效决定冲突的不同提交返回 `ErrReviewConflict`。
- 指派了审核者的变化不能再走匿名的 `confirm-revision`/`cancel-revision`（返回 `ErrReviewerRequired`）；未指派的变化继续使用旧命令，兼容原流程。
- 审核责任、生效决定与 `reviews` 执行记录随状态文件持久化，保存重开后可继续处理；已有修订、历代 artifact、待决外部事项和 attempt 历史一律保留。

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
go run ./cmd/jobctl revisions
go run ./cmd/jobctl propose-revision --spec-file changes.json --note 'fix upstream output'
go run ./cmd/jobctl preview-revision <revision-id>
go run ./cmd/jobctl confirm-revision --indexes 0 <revision-id>
go run ./cmd/jobctl cancel-revision --indexes 1 <revision-id>
go run ./cmd/jobctl review-revision --reviewer alice --decisions 0:ACCEPT,1:RETURN --reason 'checked' <revision-id>
go run ./cmd/jobctl resolve-pending --status CONFIRMED --resolution 'verified webhook' <pending-id>
go run ./cmd/jobctl compare-revision <revision-id>
go run ./cmd/jobctl recover
```

`changes.json` 是 `ChangeSpec` 数组，例如：

```json
[
  {
    "JobID": "job-1-...",
    "Kind": "REPLACE",
    "NewResult": "echo:corrected",
    "Reviewer": "alice",
    "ExternalEffects": ["verify that webhook idempotency key was used"]
  },
  {
    "JobID": "job-2-...",
    "Kind": "REVOKE"
  }
]
```

`worker` 默认持续轮询；没有待执行任务时也会保持运行。重试回退间隔当前为 100ms，便于内置短任务快速完成。

## 验证

```bash
go test ./...
go test -race ./...
```
