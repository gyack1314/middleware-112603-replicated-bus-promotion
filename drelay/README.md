# drelay：多区域事件接入与灾备中间件（本地仿真）

在一套本地数据目录上仿真“区域主入口 + 灾备站”的事件接入、跨区复制、
灾备提升、损失批准与消费接续。只使用 Python 3.11 标准库，无网络、
无外部依赖；区域端完全由消息驱动。

## 两类状态严格分离

| 状态 | 含义 | 生命周期 |
| --- | --- | --- |
| 接受事实 | 主站受理一次写，固定 `(epoch, position, digest, written_by)` | 只增，不随副本现状改变、不被切换删除 |
| 副本证据 | 每个位置在哪些区域有实际副本（`copied` 消息登记） | 可乱序到达；只影响确认级别与连续边界 |

因此“最大 position 已有副本”绝不等于“前面每条都存在”。所有连续
边界（确认边界、候选区域边界、跨区边界）都由位置 1 起的**前缀扫描**
得到，遇到第一个不满足条件的位置即停止。

## 三种业务回执（write 的 status）

- `waiting`：已受理但尚未满足该流的持久保证。regional 流只有主站
  副本时返回它。
- `accepted`：宽松（local）流在主站已保管后返回，级别标为
  `primary_only`，明确告知业务“仅主站”。
- `confirmed`：两个指定区域持有同一内容（跨区确认），任何策略下
  都是满足要求的持久确认。

`level` 是拓扑事实（`confirmed` / `primary_only` / `waiting` / `gap`），
`status` 是结合流策略后的业务含义。同一生产请求（write 的 `id`）
重送时返回 `deduplicated: true` 与**原 position、原 epoch、原结果**，
不会重新分配位置。

## 发布代次与位置

- position 在单一流内只增；切换后新写继续向后编号，**不重用**任何
  已经向客户端报过的位置。
- 每次提升使用严格增大的新 epoch；切换期间该流写入冻结。
- 旧主的写按顺序被 `stale_epoch`（旧代次）或 `not_primary`（陌生
  区域冒写）拒绝，恢复后也不能重新获得写权。

## 切换（promote）是两阶段、可重复调用的

1. 首次 `promote` 打开切换：阶段变为 `promoting`，逐个位置分类——
   候选区域已有证据的位置可接管，没有证据的位置标 `pending` 缺口。
2. 值班端等待仍可能恢复的证据：迟到的 `copied` 会把 pending 缺口
   转为 `recovered`；无法找回的 local 尾部走损失批准。
3. 重复调用同一 `(region, new_epoch)` 是幂等的；当不存在 pending
   缺口时切换才完成，新主取得发布资格。竞争的接管请求被
   `promotion_conflict` 拒绝。

接管可以先对满足最后强保证的流生效；`last_strong_frontier` 给出该
流最后一条连续强保证位置。

## 损失批准（loss）的硬约束

- 只能在 `promoting` 阶段、对 `pending` 位置逐 span 批准；span 中
  只要有一个位置不可批准，整笔拒绝、状态不变。
- regional 流一律拒绝（`loss_forbidden_regional`）：未达约定的已
  受理写必须先补齐，或由源端显式放弃尚未受理的写。
- 曾经跨区确认过的数据（`cross_region_frontier` 之内）不能被批准
  丢弃，即使它是 local 流。
- 批准后位置成为 `announced_gap`，审计记录操作员与时间。之后即使
  找回内容，也只作为 `late_evidence` 历史事实保存，缺口不复活、
  消费者仍会在该位置收到缺口通知。
- digest 与接受事实不符的副本触发 `digest_conflict`，证据被隔离到
  `conflicts`，同一位置永远不会出现两种结果。

## 消费接续（read / ack）

消费者带 `(after, epoch)` 重连，得到：

- `entries`：实际处理位置之后可正常续接的位置（含各自 epoch，
  跨越代次边界可见）。
- `announced_gaps`：已宣布缺口，**包含 after 本身**——客户端必须能
  发现自己手里该位置的数据在新主已缺失。
- `pending_gaps`：切换中尚未定论的位置。
- `resume_from`：服务端记录的实际消费进度（来自 ack），不会因为
  客户端重连时带了更小的 after 就声称退回未处理。
- `epoch_behind`：客户端所见代次落后于当前发布代次。

## 持久化与重启

数据目录（默认布局）：

```
<data-dir>/
  control.log            # 流登记等控制事件
  stream-<stream>.log    # 每个流的仅追加事件日志
```

每条事件 fsync 后才返回回执。`restart` 关闭句柄并从日志完整重放，
重建全部接受事实、副本证据、缺口、消费者进度、冲突与审计；被拒绝
的操作也以 `operation_rejected` 事件留痕。

## 值班端追查（inspect）

按流返回：新旧发布代次与阶段、主备区域、各确认级别、每位置的副本
集合与缺口状态、`confirmed_frontier` / `cross_region_frontier` /
`candidate_frontier`、真实缺失与恢复状态、消费者进度、冲突清单和
完整审计轨迹。

## 用法

运行演练资料（每个场景使用独立数据目录）：

```
python3 -I drelay_console.py --data-dir ./data run examples/replication-hole.jsonl
```

单步操作：

```
python3 -I drelay_console.py --data-dir ./data register \
    --stream s --tenant lab --primary a --secondary b --policy regional
python3 -I drelay_console.py --data-dir ./data write \
    --stream s --region a --epoch 1 --id w1 --digest h1
python3 -I drelay_console.py --data-dir ./data copied \
    --stream s --region b --source-epoch 1 --position 1 --digest h1
python3 -I drelay_console.py --data-dir ./data promote \
    --stream s --region b --new-epoch 2
python3 -I drelay_console.py --data-dir ./data inspect --stream s
```

测试：

```
python3 -I -m unittest discover
```
