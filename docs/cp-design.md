# poly-config CP（Raft）设计

> 状态：CP-1/CP-2/CP-3 已完成（日志底座、选举、复制与提交），进行中 CP-4（读与快照）
> 更新：2026-11-08
> 前置阅读：product-form.md（CP 路线 = 手写 Raft）、ap-design.md（工程模式参照）

## 1. 形态定案

| 维度 | 决策 |
|---|---|
| 一致性算法 | 手写 Raft：单 leader、多数派提交、日志复制、崩溃恢复 |
| 分区粒度 | 无分片——**一个 Raft group 管全部配置**（与 AP 的分区写责任不同） |
| 写 | 只有 leader 提交；任意节点收写后内部转发 leader |
| 读 | 默认 LOCAL（任意节点本地直读，leader 不参与）；显式 LINEARIZABLE 走 read-index |
| 订阅/推送 | 与 AP 完全一致：每节点本地订阅表，apply 即推送，订阅不跨节点 |
| leader 故障 | 选举自动换代（约 1~2 个选举超时），已提交数据不丢 |
| 成员管理 | 静态配置（复用 `--cluster-members`）；**v1 不实现成员变更协议**，变更走停写维护窗口 |
| 定位 | 与 AP 并列的部署时切换模式：AP 赢在分片写并行，CP 赢在不丢/秒级换主/确定性恢复 |

推论：

- 单写者模型下**集群不提升写吞吐**（leader 单点写，AP 的 owner 同理）；CP 集群的收益是
  可用性（换主自愈）、读扩展（LOCAL 读任意节点）与数据冗余。
- Raft 必须做对的三件事：**选举安全、提交安全（本 term 限定）、apply 确定性**；
  工程上必须守住的一条铁律：**Raft 状态（term/role/votedFor/commitIndex/日志指针）
  的一切读写串行化到单线程信箱**，Netty 线程只收发字节、进状态机前先排队。

## 2. 一致性语义（保证清单）

| 保证 | 来源 |
|---|---|
| 已应答的写不丢 | 应答 = 多数派落盘 + commit + apply（D7） |
| 无冲突版本、无丢失更新 | leader 单点串行分配 per-key version；expectedVersion 乐观锁不变 |
| 客户端视角不回退 | SDK 按 per-key version 单调守卫（与 AP 同一套） |
| 写后即读（写经过的节点） | 转发节点收到 leader 应答（=已提交）后**先本地 apply 再回复客户端**（D7） |
| 同连接/同节点读单调 | 状态机只向前 apply；写应答前已 apply |
| LINEARIZABLE 读 | read-index：基线 commitIndex + 心跳确认领导权 + 等 apply（D1/D2） |
| 崩溃恢复确定性 | 重启 = 回放已提交日志 + 快照，**无收敛窗口**（对比 AP 反熵） |

不保证：

- 默认 LOCAL 读的新鲜度：落后一个复制 RTT，由 push + 批量对账收敛（有界、不回退）。
- 其他节点的写后即读：跨节点读到刚完成的写可能差一个 RTT，与 AP 语义一致。

网络分区下的性质：

- 少数派侧：写提交不了（凑不齐多数派），命令卡在"已记账未盖章"，一条不丢；读按
  LINEARIZABLE 则失败、LOCAL 则读到旧值。
- 多数派侧：正常选主写入，分区愈合后少数派的未提交尾巴被合法截断覆盖。
- 旧 leader 回归：第一条消息看到更高 term 立即退位（规则一），不存在双主。

## 3. 选举

沿用 Raft 标准机制（细节讨论记录见决策 D9）：

```
Follower ──选举超时(随机 [t,2t), 默认 1000~2000ms)无心跳──▶ Candidate(term+1, 先投自己, 落盘)
   ▲                                                │
   │                    多数派赞成 ─────────────────▶ Leader(立刻心跳压制)
   │                                                │
   └──── 收到更高 term / 新 leader 心跳 ────────────┘（回退 Follower）
```

> 超时默认值取 etcd 量级（1000ms 基数、心跳 100ms，可配
> `--raft-election-timeout-ms` / `--raft-heartbeat-ms`）：论文的 150~300ms
> 对 Java GC 停顿过于激进；测试统一调小到 300/50 换取秒级可观测的换代。

- **规则一（term 比对）**：任何消息先比 term，高者赢、低者忽略、本地 term 永不回退；
  收到更高 term 立即更新并回退。选举与复制（AppendEntries）共用同一入口逻辑。
- **规则二（身份校验）**：计票前检查 `role == CANDIDATE && term == electionTerm`，
  任一不符该票作废——票绑定"届"，跨届/退位后的迟到票一律丢弃。
- **投票两道闸**：term 合法 → 本 term 未投他人 → 候选人日志"够新"
  （(lastLogTerm, lastLogIndex) 比较）→ 落盘 votedFor 后才回复。
- **先落盘再发消息**：currentTerm/votedFor 变更必须 fsync 后才回复投票或发起拉票。
- 随机超时错开首轮；一轮不中下一轮重抽。多数派 = `floor(n/2)+1`，静态成员表
  按 nodeId 排序保证各节点算出同一张表（复用 `ClusterMember` 解析）。
- Leader 当选后追加一条 **no-op 条目**（本 term 空命令）——它被提交时，之前的
  旧 term 条目连带提交（提交安全规则见 §5）。

## 4. 写链路（发布/删除）

```
客户端/控制台 ──发布──▶ 任意节点
                          │ 非 leader
                          ▼ RAFT_FORWARD
                        Leader
                          ① 单线程信箱内：D5 水位校验 expectedVersion
                          ② 分配 per-key version（写进 assignedVersion）
                          ③ 追加 RaftLog（term, index, 命令）+ 本地刷盘
                          ④ 并行 AppendEntries → 全体 follower
                          ⑤ 多数派回执 → commitIndex 前进（本 term 限定，见 §5）
                          ⑥ apply 进状态机（触发本地推送）
                          ⑦ 应答转发节点（应答 = 已提交，附 mutation + index）
                          ▼
                        转发节点：applyReplicatedIfNewer 幂等落地 → 回复客户端
                          （写后即读闭环；稍后 AppendEntries 送达同一 index 时幂等跳过）
```

要点：

- 发布直接打到 leader 本机时省略转发，其余相同。
- **应答条件 = commit + apply**（D7）；复制失败不重试，下一轮心跳补（nextIndex 未动，
  幂等），与 AP"校验补齐"的容错哲学一致。
- 转发节点的提前 apply 是安全的：leader 应答 = 条目已在多数派落盘 = 永不改写；
  其后 Raft 正常投递同一 index 时被 version/幂等守卫空操作。
- at-least-once：转发响应丢失但 leader 已提交时，客户端重试会再写一次（内容相同、
  version+2），与 AP 同款语义。
- leader 不可达/选举中 → `UNAVAILABLE`（HTTP 503）带明确错误信息；版本冲突 409、
  NOT_FOUND 404 不变。

### D5：propose 的校验时机（在途叠加水位）

M0/AP 是"锁内校验→赋版本→追加→apply"一气呵成；CP 下 commit 才 apply，存在缝隙：
写 A 已追加未提交时，写 B 若只对着已 apply 状态校验会与 A 撞版本。方案：

> RaftNode 单线程内维护「**已 apply 状态 + 本 leader 在途条目的顺序叠加水位**」：
> propose 按序处理——对叠加视图做 expectedVersion 校验、分配 version、追加；
> commit/apply 后水位并入正式状态机；**step-down 时丢弃在途水位**，在途请求一律
> 失败（客户端重试新 leader）。赋版本永不碰撞，校验语义与 M0 完全一致。

## 5. 日志复制与提交

### 5.1 日志结构

一条 Raft 日志 = **命令 + 身份证**：

```
index（第几条，全集群同号同内容） + term（生于哪一朝） + payload（ConfigCommand）
```

- **index** 是账本页码：commitIndex/matchIndex/nextIndex/快照点全部以它为坐标；
  CP 下同时履行 revision 的角色（节点本地序号）。
- **per-key version** 与 index 解耦：version 是"这个配置第几次修改"（跨节点权威，
  客户端守卫用），index 是"第几条命令"（复制提交用）。leader 赋 version 写进
  `ConfigCommand.assignedVersion`（AP-2 已铺好的管道），follower/回放直接用。
- 删除 = 带 version 的墓碑命令，走同一链路（与 AP 一致）。

### 5.2 复制流程（AppendEntries 三闸）

```
Leader 给每个 follower 单独组装（逐 follower 的 nextIndex）：
  prevLogIndex = nextIndex[i] - 1
  prevLogTerm  = leader 自己日志[prevLogIndex].term
  entries      = 日志[nextIndex[i] .. ]

Follower 处理：
  闸1 term：对方 < 我 → 拒绝并回带自己的 term（教化旧 leader 退位）；≥ 我 → 追平/重置计时器
  闸2 对齐：我第 prevLogIndex 号的 term == prevLogTerm？不符 → 拒绝（回带 lastLogIndex/lastLogTerm 线索）
  闸3 追加：冲突尾巴整段截断（truncateAfter）→ 换上 leader 条目 → fsync 后回 true
```

- 回退：闸2 被拒 → `nextIndex--`（可配合 follower 回带的线索跳跃/批量回退），
  找到共同前缀为止；**退过快照边界则停止回退，改发 InstallSnapshot**（§7）。
- 并行流水线：各 follower 独立推进；单 follower 回执丢失不重试，下轮心跳自然补上
  （幂等：闸2 会处理重复）。攒批发与组刷盘合并（一轮 flush 服务本地追加与
  follower AppendEntries）。
- 日志匹配不变量：同 index 同 term ⇒ 之前全部相同——闸2 只核对接缝一个点。

### 5.3 提交规则（必须做对的一条）

```
commitIndex = max { N | 多数派 matchIndex ≥ N 且 log[N].term == currentTerm }
```

- **只能直接提交本 term 的条目**；旧 term 条目通过本 term 的 no-op 连带提交。
  否则存在"已提交条目被后续 leader 覆盖"的 Figure 8 事故。
- commit 推进后，leader/follower 从 `(lastApplied, commitIndex]` 确定性 apply 进
  `ConfigStateMachine`（apply 即推送，钩子不动），并放行挂在该区间上的
  写 future / 读等待器。

## 6. 读链路

| 级别 | 路径 | 语义 |
|---|---|---|
| **LOCAL（默认）** | 任意节点读本地状态机，leader 零参与 | 有界陈旧（复制 RTT 级），push+对账收敛，同节点单调不回退 |
| **LINEARIZABLE（显式）** | 非 leader 转发 leader → read-index → 返回 | 返回值 ≥ 此前已完成的全部写 |

### read-index（LINEARIZABLE 的实现，D2）

```
leader 收到读：
  ① readIndex = commitIndex（信箱内取，天然无锁）
  ② 向多数派确认领导权（心跳轮，可与周期心跳合并，失败超时兜底）
  ③ 等 lastApplied ≥ readIndex
  ④ 读本地状态机返回
```

- 不写日志、无磁盘动作；多数情况搭周期心跳的车。leader 切换时在途读全部作废可重试。
- 不做 lease read（不引入时钟假设）。
- 优于 ZK 的 sync+读两轮往返：读 RPC 直接带 `LINEARIZABLE` 标志，服务端内部
  完成确认，一轮往返（D8）。

### 客户端 API（D8）

```java
// 默认 LOCAL —— 常规调用
String getConfig(String ns, String group, String dataId, long timeoutMillis);
// 显式强一致 —— "写完必须读准"的调用方
String getConfig(String ns, String group, String dataId,
                 long timeoutMillis, ReadConsistency consistency);
```

`ReadConsistency`（LOCAL/LINEARIZABLE）已存在；服务端 `protocol.read(key, LINEARIZABLE)`
路径已存在（AP 转发 owner，CP 改为转发 leader + read-index）。HTTP 可选
`?consistency=linearizable`，默认不带 = LOCAL。控制台日常浏览走 LOCAL——
"发布完刷新看不到新值"由写后即读（同节点）兜住。

### 控制台请求路径

| 控制台操作 | 路径 |
|---|---|
| 发布 / 删除 / 回滚 / 创建 ns/group | 当前节点收 HTTP → 转发 leader（与普通写同链路） |
| 配置列表 / 内容 / 历史 | 当前节点本地读（同源：写后即读保证刷新可见） |
| 集群状态页 | 当前节点采集（复用状态采集通道，CP 见 §10 RAFT_STATUS） |

## 7. 快照与压缩

- 状态机快照复用 `SnapshotStore`（原子写、保留 2 份、formatVersion），**元数据区
  增加 `lastIncludedIndex` / `lastIncludedTerm`**——快照视作该位置的一条伪日志条目，
  用于回退匹配点校验。
- 触发沿用 `--snapshot-threshold`；压缩走 `SegmentLog.deleteSegmentsBefore`
  （RaftLog 换算为覆盖物理位点）。
- **InstallSnapshot**：follower 落后到 leader 快照边界以内时，停止 nextIndex 回退，
  整份快照发过去（携带 lastIncludedIndex/Term）；follower 丢弃本地日志、加载快照、
  从 `lastIncludedIndex+1` 继续追日志。
- 重启恢复：加载最新快照 → 从 `lastIncludedIndex+1` 回放 RaftLog 增量 →
  `(lastApplied, commitIndex]` 补 apply。**恢复是确定性的，没有 AP 的收敛窗口。**

## 8. 版本、删除与元数据

| 项 | 决策 |
|---|---|
| index | Raft 全局日志序号，复制/提交坐标，**同号必同内容**（已提交前缀） |
| version | per-key，leader 分配，跨节点权威单调——客户端守卫/推送/对账基准不变（与 AP 同一套） |
| revision | = raft index（节点本地日志位点，各节点不同，仅本地有意义） |
| assignedVersion | leader propose 时写入命令，follower/回放直接用（AP-2 已有字段，编码不变） |
| 删除 | 带 version 的墓碑命令，走同一复制链路；墓碑 GC v1 不做（与 AP 对齐） |
| workspace 元数据 | 与 AP 相同的内置分区标记条目方案，走 Raft 复制（复用 AP-3 实现） |
| expectedVersion | leader 在 D5 叠加视图上校验；-1 必须不存在语义不变 |

## 9. 内部协议（4xxx 段）

复用 remoting 端口（与 Distro 3xxx 并存，按 mode 只注册本模式需要的处理器）：

| 协议码 | 用途 |
|---|---|
| RAFT_VOTE (4001) | RequestVote：拉票（term、候选人日志尾信息、是否 preVote 预留） |
| RAFT_APPEND (4002) | AppendEntries：复制 + 心跳（空 entries 即心跳；应答回带 term/冲突线索） |
| RAFT_SNAPSHOT (4003) | InstallSnapshot：整份快照下发（lastIncludedIndex/Term + 数据） |
| RAFT_FORWARD (4004) | 写转发：任意节点 → leader（应答带 mutation + index，供转发节点先 apply） |
| RAFT_READ (4005) | LINEARIZABLE 读：任意节点 → leader（read-index 在 leader 侧完成） |
| RAFT_STATUS (4006) | 状态采集：成员自报（role/term/commitIndex/lastApplied/peer matchIndex），控制台聚合 |

消息一律**先入 RaftNode 单线程信箱再处理**（同进程消息也走队列，不绕过串行化）。

## 10. 与现有代码的关系

```
ConfigProtocol
 ├── LocalProtocol  → standalone（不动）
 ├── DistroProtocol → ap（不动）
 └── RaftProtocol   → cp（新）── RaftNode（单线程信箱：选举/复制/commit/apply 调度）
                                        │
              ┌─────────────┬───────────┴────────┬──────────────┐
              ▼             ▼                    ▼              ▼
          RaftLog      ConfigStateMachine   SnapshotStore   组刷盘线程
       (底座 SegmentLog)      （复用）         （复用+快照点）  （升级：组刷盘+组复制）
```

| 零件 | CP 用法 |
|---|---|
| `SegmentLog` | RaftLog 底座：追加/读写锁/组刷盘/截断/前缀删除原样用；payload 前缀带 index+term |
| `SnapshotStore` | 状态机快照原样用，加 lastIncluded 元数据（CP-4） |
| `ConfigStateMachine` | 原样用；apply 即推送；verifyTable/墓碑等 AP 产物 CP 不需要 |
| `LocalEngine` | **propose 编排不复用**——"追加即生效"与"commit 才生效"是两种编排，硬合会搅浑语义；零件（状态机/快照/组刷盘）复用，编排在 RaftNode 新写（见 ap-design §10 同款教训：共用零件、不共用流程） |
| Distro 工程模式 | forward、per-peer 复制器、processor 注册、健康时间戳、状态页——结构照搬，协议码换 4xxx |
| `--cluster-members` / `ClusterMember` | 静态成员表、多数派计算直接复用 |

数据目录布局（CP 模式）：

```
<data-dir>/
  raft/
    log/        ← RaftLog 的 SegmentLog 底座
    meta.json   ← currentTerm + votedFor（原子写 + fsync，回复任何投票/复制消息前落盘）
  snapshot/     ← 状态机快照（复用，CP-4 起携带 lastIncluded）
```

## 11. 里程碑（每个可独立验收）

| 里程碑 | 内容 | 验收 |
|---|---|---|
| CP-1（✅ 2026-11-07） | `RaftLog`：帧带 index/term、尾部截断 `truncateAfter`、前缀压缩接入、索引重建；`raft-meta` 持久化（fsync 语义）；恢复回放 | 单机写→崩溃重启逐号恢复；冲突尾巴截断后重启不复活；meta 丢失回退不越权 |
| CP-2（✅ 2026-11-07） | `RaftNode` 单线程信箱 + 三角色；RequestVote 两道闸；随机超时/心跳压制；`--mode=cp` 装配放行（先单节点） | 单节点 cp 启动写通；3 节点选出唯一 leader；杀 leader 毫秒级重选；双投/双主单测打不穿 |
| CP-3（✅ 2026-11-08） | AppendEntries 三闸 + nextIndex 回退；commit 本 term 限定 + no-op；commit→apply 循环（推送钩子）；D5 水位；RAFT_FORWARD 写转发 + expectedVersion；组刷盘升级 | 写任意节点→全集群提交可读、三节点客户端收推送；409 照常；2/3 宕机写不可用但一条不丢、恢复自愈；图 8 用例固化成测试 |
| CP-4 | read-index（LINEARIZABLE）；快照带 lastIncluded；InstallSnapshot 追平落后 follower | 线性一致读测试（选举窗口不回退）；快照压缩后杀 follower→重启一发追平；滚动重启收敛 |
| CP-5 | SDK `getConfig` 加 ReadConsistency 重载（D8）；错误语义（选举中/无主提示）；RAFT_STATUS + 控制台 CP 视图（leader/term/commitIndex/peer matchIndex）；三节点测试矩阵；文档定稿 | 完整演示：杀 leader 换代、写读按语义正确；矩阵全绿；本文档补实施记录 |

顺序用意：CP-1/CP-2 先把"账本和朝代"做对（纯本地、无网络语义，地基）；CP-3 是
主干且是 D5 语义缝的落点；CP-4 在正确性之上加能力；CP-5 照 AP-5 套路收尾。
难点集中在 CP-2/CP-3（选举状态机与 commit 规则是纯新增逻辑），CP-1/4/5 多为
组装与复用。

### CP-1 实施记录（2026-11-07，已完成）

- **RaftLog**（`core/raft`，底座 `SegmentLog`）：记录前缀 16B（index 8B + term 8B），
  `append` 恒从 lastIndex+1；`termAt` 内存平行表 O(1) 不落盘；`truncateAfter` 冲突
  截断（物理+内存同步裁剪，重启不复活）；`compactBefore` 前缀压缩（删完整段 +
  按 `getMinOffset` 修剪内存表，残留物理条目由快照位点裁决）；恢复扫描校验 index
  **逐号连续，不连续拒绝启动**（SegmentLog 只保留连续有效前缀，不连续只可能是
  逻辑错误或介质损坏）。`SegmentLog` 增 `getMinOffset()`（压缩后非零起点恢复用）。
- **RaftMetaStore**：currentTerm/votedFor，"临时文件 → fsync → 原子替换"，
  **返回即落盘**（先落盘再投票的载体）；term 回退拒绝；文件损坏/格式不符/负 term
  一律拒绝启动（带着丢失的 votedFor 起动可能同届重复投票——宁起不来不投错票）。
- 测试：`RaftLogTest` 8（逐号恢复、截断持久化、越界拒绝、压缩+非零起点恢复、
  index 跳号拒启、过短记录拒启、目录锁）、`RaftMetaStoreTest` 5（重载、不回退、
  同届改投分层放行、三种损坏拒启）。

### CP-2 实施记录（2026-11-07，已完成）

- **RaftNode 单线程信箱**（§1 铁律落地）：1 线程 `ScheduledExecutorService` 同时
  承担信箱与定时器——入站消息（网络 RPC、propose）先投递成任务，选举/心跳定时器
  同线程调度，状态变更天然互斥、无锁。跨线程只保留一个 `volatile closed`
  （停机开关，唯一由状态线程之外写入的标志）。
- **选举**（§3 落地）：随机超时 `[t,2t)`（默认 t=1000ms，可配，测试 300/50）；
  `startElection` = term+1 → 自投 → **persist fsync 先于拉票** → 并行 RequestVote；
  投票两道闸（一 term 一票、候选人日志够新）+ **落盘先于回票**；规则一（应答/消息
  带更高 term 立即退位、本地 term 永不回退）、规则二（计票校验
  `role==CANDIDATE && electionTerm==currentTerm`，退位/换届后的迟到票作废）；
  当选追加 **no-op** 并立刻心跳压制。
- **心跳**：实体 = 空条目 AppendEntries（4002，与复制同一消息格式，逐 peer 的
  prev 凭据随行）；发送 `RaftNode.sendHeartbeats`（状态线程定周期），接收端闸1 通过
  即 `becomeFollower → resetElectionTimer`——**心跳压制就落地在这一行**。
- **提交（单成员路径）**：`tryCommit` = 多数派 matchIndex + **term == currentTerm
  限定**（Figure 8）；`applyCommitted` 逐条 apply（空载荷 no-op 跳过），完成挂起的
  propose future。
- **RaftProtocol**：propose 的校验+赋版本在状态线程内按序执行（D5；单成员下"已
  apply 视图"即正确，多成员叠加水位留 CP-3）；**version = raft index**（no-op 占
  1 号，首写 version=2，与 AP"version=写入方全局序号"同语义）；恢复：单成员
  `commitIndex = lastIndex` 构造期即 apply——**重启读路径无空窗**，多成员为 0 等
  leader；读/列表/历史直走状态机，workspace 标记条目与 AP 同套派生。
- **装配**：`ServerOptions` 增 `--raft-election-timeout-ms`（默认 1000，须 ≥100 且
  ≥2 倍心跳）、`--raft-heartbeat-ms`（默认 100）；CP 与 AP 同规格强制成员表含本节点；
  `PolyConfigServer` 放行 `mode=cp` 并注册 RAFT_VOTE(4001)/RAFT_APPEND(4002)
  处理器（编解码 → 投信箱 → future.get(2s) → 回包，fsync 时序由该等待保证）。
- **范围守卫**：多节点 propose 明确报"CP 多节点写复制由 CP-3 提供"；收到带条目的
  AppendEntries 打 warn 不应用——代码内 TODO 标注 CP-3 接入点。
- 测试：`RaftNodeTest` 11（投票矩阵、规则一/规则二迟到票作废、单节点选主+写、
  三节点唯一 leader + 同 term 双主采样 + 杀主换代、范围守卫、恢复无空窗），
  `CpClusterIntegrationTest` 2（单节点写通 + 409 + **重启后不等选主立即读到已应答
  写**；三节点选主→杀 leader→存活节点换代且 term 更高），`ServerOptionsTest` +5；
  全量 **123 通过**。Java 8 API 合规经 `--release 8` 严格编译验证（修正
  `List.copyOf`/无参 `orElseThrow` 两处 Java 10 API——JDK17 下 `-source 8` 不带
  `--release` 会假绿，须以 `--release 8` 为准）。
### CP-3 实施记录（2026-11-08，已完成）

- **三闸落地**（`RaftNode.handleAppendRequest`）：闸1 term（过期 leader 拒绝回带
  本朝 term）；闸2 prev 接缝（拒绝回带 lastLogIndex/lastLogTerm 线索）；闸3 =
  连续性+Base64 **全量预检（先校验后动日志，失败不留半截状态）** → 冲突尾巴
  `truncateAfter` 截断 → 追加 → 一轮 fsync 后才回 true。重复条目（同号同 term）
  幂等跳过。`leaderCommit` 传播：`commitIndex = min(leaderCommit, lastIndex)` 推进
  后走同一套 commit→apply（推送钩子不变）。
- **复制与回退**：propose 追加后立即 `replicateToPeers`（不等心跳）；心跳 = 按
  逐 peer `nextIndex` 组装的 AppendEntries（空条目即心跳，单条批量上限 256）。
  应答处理：success → matchIndex 只增不减（防乱序旧应答）→ tryCommit → 仍有缺口
  立即续传；failure → `min(next-1, 回带线索+1)` 跳跃回退、保底退 1、立即重发
  （prev=0 必对齐，回退有下界不会死循环）。迟到应答（term 落后本朝）作废。
  同 peer 并发发送可能乱序——位点推进用 max/幂等兜底，安全但偶发冗余重发，
  per-peer 串行化留后续打磨。
- **组刷盘升级**：propose 的本地追加不立即 fsync，入队尾部"刷盘+提交"任务——
  队列中后续追加被同一轮 fsync 覆盖（多写共享一次磁盘动作）；follower 侧一轮
  AppendEntries 全部条目只 fsync 一次。**先持久化再计票**：leader 自身
  matchIndex 只在 fsync 完成后推进（未刷盘不计入多数派）。
- **D5 在途叠加水位**：`RaftProtocol.pendingWrites`（index → key+version，
  并发容器）——propose 的 prepare 在状态线程内"先查叠加水位再 put"，校验底账 =
  已 apply 状态机 + 本 leader 在途写；propose 完成（apply 成功 / step-down 失败）
  即移除；**step-down 时 `failWaiters` 使在途写一律失败**（客户端重试新 leader）。
  写提交超时 = `max(3s, 3×选举超时)`：超时只放弃客户端等待，条目与水位保留到
  apply/step-down（恢复后照常提交，at-least-once）。
- **RAFT_FORWARD（4004）**：非 leader 收写 → 转发 leader `proposeLocal`（角色闸
  拒绝、不二次转发）；leader 应答 = commit+apply（D7），携带 mutation+index；
  转发节点按该 version **提前 apply**（写后即读闭环），随后 AppendEntries 送达
  同 index 时幂等跳过。幂等三闸（`applyFromLog`/`applyForwarded` 方法级互斥）：
  per-key 提前 apply 水位（挡同条重放与低 index 旧条晚到）+ 状态机已有同版或
  更新 + 删除遇 key 已不存在；转发路径另以 `logAppliedRevision` 连续位点判
  "日志已按序走过本条"。错误码映射与 AP 转发同款（409/404/500→UNAVAILABLE）。
- **装配**：`PolyConfigServer` 在 mode=cp 注册 RAFT_FORWARD 处理器
  （`RaftForwardProcessor`，与 DistroForwardProcessor 同构）。
- **测试**（全量 **129 通过**）：
  - `RaftNodeTest` 13：CP-2 的 11 项 + 多节点复制提交、落后 follower 经
    nextIndex 回退追平、**图 8 用例**（孤立 leader 的旧 term 在途写不提交、
    换代后被截断不复活、退位时在途写失败——`DirectNetwork` 新增
    isolate/heal + 发送方视图 `viewOf` 支撑非对称分区）。
  - `CpClusterIntegrationTest` 6：CP-2 的 2 项 + 写任意节点→全集群可读 +
    409 经转发、2/3 宕机写 503 + 恢复一条不丢自愈、**D5 在途水位**
    （-1 被在途写挡住 409 即时返回、对准在途 version 放行后超时、恢复后
    超时写照常提交 version 链正确）、三节点变更监听全触发（apply 即推送钩子）。
- **测试基建教训**：`PolyConfigServer.stop()` 是 2~7s 的优雅停机，窗口内被停
  节点的 Raft 仍活着会参与选举、把真 leader 拉下马——故障注入须用
  `killServer`（先秒级 `getConfigService().close()` 关协议、再 stop 清理网络；
  协议 close 幂等）。

### 测试策略

- 单测打状态机与闸门：投票决策矩阵、闸1/闸2 拒绝与回退、commit 本 term 限定、
  D5 水位、RaftLog 截断/恢复。
- 集成测试起真三节点（沿用 ApClusterMatrixTest 的端口段套路）做故障注入：
  停 leader、停多数派、杀 follower 后追平、滚动重启。
- 不写分布式模拟器；确定性场景靠单测状态机，真实时序靠集成测试。

### v1 明确不做（范围护栏）

动态成员变更（joint consensus / 单步变更）、pre-vote、lease read、learner/只读
副本、leader 主动转移、多 raft group、读管道优化（批量化 read-index）——全部留
后续里程碑。成员表变更在 v1 走停写维护窗口（静态成员，product-form 既定）。

## 12. 决策记录

| 日期 | 决策 |
|---|---|
| 2026-11-07 | 手写 Raft，单 group 全量配置；与 AP 并列部署时切换，不互相替代 |
| 2026-11-07 | Raft 状态一律单线程信箱串行化；网络线程只收发字节 |
| 2026-11-07 | **D1** CP 默认读 LOCAL（ZK 派，读扩展优先）；强一致读 SDK 显式方法 |
| 2026-11-07 | **D2** 强一致读用 read-index（一轮往返，优于 ZK sync+读两轮）；不做 lease（不引入时钟假设） |
| 2026-11-07 | **D3** RaftLog 新接口、底座复用 SegmentLog；raft-meta（currentTerm/votedFor）JSON 原子写 + fsync，回复消息前落盘；commitIndex 不持久化 |
| 2026-11-07 | **D4** version 语义不变：per-key、leader 经 assignedVersion 分配；revision = raft index |
| 2026-11-07 | **D5** propose 校验基于「已 apply + 本 leader 在途叠加水位」，step-down 丢弃在途、请求失败重试 |
| 2026-11-07 | **D6** 单节点 `--mode=cp` 合法（自投票、多数派=1 立即提交），服务本地开发与测试 |
| 2026-11-07 | **D7** 写应答 = commit + apply 之后；转发节点收到 leader 应答（=已提交）先幂等 apply 再答客户端（写后即读） |
| 2026-11-07 | **D8** SDK 增加 `getConfig(..., ReadConsistency)` 重载，默认 LOCAL；HTTP 可选 `?consistency=` |
| 2026-11-07 | **D9** 选举沿用 Raft 标准：随机超时 [t, 2t)、默认 t=1000ms、心跳 100ms（可配 `--raft-election-timeout-ms` / `--raft-heartbeat-ms`，须 ≥100ms 且 ≥2 倍心跳）、no-op 当选、规则一/规则二双保险；默认值取 etcd 量级而非论文 150~300ms（Java GC 停顿），测试调小到 300/50 |
| 2026-11-07 | 提交规则带本 term 限定（Figure 8）；旧 term 条目经 no-op 连带提交 |
| 2026-11-07 | 复制失败不重试，nextIndex 未动靠下轮心跳幂等补上；回退带 follower 线索优化，退过快照边界转 InstallSnapshot |
| 2026-11-07 | LocalEngine 的 propose 编排不复用；零件（状态机/快照/组刷盘）复用，编排在 RaftNode 新写 |
| 2026-11-07 | 静态成员复用 `--cluster-members`；v1 成员变更走停写维护窗口，成员变更协议归后续里程碑 |
| 2026-11-07 | CP/AP 取舍明示：AP 赢分片写并行，CP 赢不丢/换主/确定恢复；写吞吐需求选 AP，强一致/高可用写选 CP |
