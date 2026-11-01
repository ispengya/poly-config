# poly-config AP（Distro）设计

> 状态：流程层面设计已确认，可进入工程（对应 product-form.md 的 M2）
> 更新：2026-11-01
> 前置阅读：product-form.md（AP 路线 = Distro 式）

## 1. 形态定案

| 维度 | 决策 |
|---|---|
| 副本模型 | **全量副本 + 分片写责任**：每个节点持有全量配置数据；「分片」只切写责任，不切数据 |
| 分区粒度 | `namespace + group` |
| owner 计算 | `hash(分区) % 成员排序列表`，成员为静态配置，所有节点计算结果一致 |
| owner 职责 | 分区的唯一写入口、per-key 版本分配者 |
| 读 | 任意节点本地读（默认 LOCAL）；LINEARIZABLE 转发 owner |
| 订阅/推送 | 每节点全量订阅表，本地推送；订阅不跨节点转发 |
| owner 故障转移 | **不做**。owner 宕机期间该分区写不可用；转移/选举问题归 CP（Raft）里程碑 |
| 成员管理 | 沿用静态配置，变更需滚动重启 |

推论：

- 全量副本的红利：读和推送零转发，M0 的订阅/推送/批量对账链路原样保留。
- AP 新增的只有三件事：**写转发、节点间复制、周期校验**。
- 「写入口死了谁接」本质是选举问题，交给 CP 模式解决；AP 不引入 epoch/双写防护。

## 2. 一致性语义（保证清单）

| 保证 | 来源 |
|---|---|
| 最终一致 | 异步复制 + 周期校验兜底，收敛通常毫秒级，校验周期是收敛上界 |
| 无冲突版本、无丢失更新 | 每 key 只有 owner 一个写入口、单点分配版本；配合 expectedVersion 做乐观锁 |
| 已应答的写不丢 | owner 刷盘后才应答 |
| 客户端视角不回退 | SDK 按 per-key version 单调守卫 |
| 写后即读（写经过的节点） | 转发节点收到 owner 应答后**先本地 apply 再回复客户端**，零额外网络往返 |

不保证：

- 其他节点的写后即读：写经 n1 → owner，随后从 n2 LOCAL 读可能仍是旧值（异步追赶）。
- 副本读的时延硬上界：只有校验周期是事实上的收敛上界。

网络分区下的性质（无需额外机制）：

- owner 被隔离到只剩自己时**继续接受写是安全的**——版本分配权静态绑定，隔离另一侧没有写入口，不存在双写。
- 分区期间隔离侧保持可用（AP 的取舍），另一侧对该分区写不可用；愈合后校验拉齐，无冲突收敛。

## 3. 写链路（发布/删除）

```
客户端/控制台 ──发布──▶ n1（任意节点）
                          │ hash(分区)=n2，n1≠n2
                          ▼ DISTRO_FORWARD
                        n2 (owner)
                          ① 锁内校验 expectedVersion
                          ② 分配 version（该 key 当前版本 + 1）
                          ③ 追加本地日志 + 组提交刷盘
                          ④ 刷盘后 apply 进内存状态机
                          ⑤ 应答 n1（应答中附带 mutation）
                          ▼
                        n1 收到响应：本地 apply → 回复客户端（写后即读）
                          │
                          ⑥ 异步：攒一批 mutation，DISTRO_SYNC → 其余副本
                          ▼
                        各副本：逐条比 per-key version，新则
                        追加自己的日志 + apply + 触发本地推送
```

要点：

- 发布直接打到 owner 本机时省略转发，其余相同。
- owner 应答 = 已持久化；复制异步，副本短暂落后是预期内状态。
- 复制幂等：副本只认「比本地新就覆盖，比本地旧就丢弃」，重复、乱序无害。
- 转发响应丢失但 owner 已落盘时，客户端重试会再写一次（at-least-once，内容相同、version +2），无害。

## 4. 读链路

- `LOCAL`（默认）：任意节点读本地内存状态机，控制台与 SDK 均默认此级别。
- `LINEARIZABLE`：转发 owner 读。所有写经 owner 串行分配版本，读 owner 即该分区最新已提交值。

### 控制台请求路径

控制台无特殊身份：浏览器只访问当前节点的同源 HTTP，后续路径由服务端决定。

| 控制台操作 | 路径 |
|---|---|
| 发布 / 删除 / 回滚 / 创建 ns/group | 当前节点收 HTTP → 查 owner → 转发 owner（与普通写同链路） |
| 配置列表 / 内容 / 历史 | 当前节点本地读（全量副本，含全量历史） |
| 集群状态页 | 当前节点向**全体**对等节点收集状态再聚合（与 owner 无关；内部通道的第四种用途：状态采集） |

说明：

- 修正 product-form.md §12「控制台 LINEARIZABLE 读 owner」的旧计划：全量副本下控制台读本地即可，无须转发。
- 「发布完刷新看不到新值」由写链路的「转发节点先 apply 再应答」兜住：发布连的节点在应答时已有该数据，刷新必然可见（写后即读闭环），不需要 LINEARIZABLE。
- 历史在各节点收敛：复制批次同样落各节点日志，回放重建的历史全量一致（revision 字段为节点本地序号，各节点不同，属预期）。

## 5. 订阅/推送链路

- 客户端连哪个节点，就由哪个节点推；订阅表 = 「我的连接上订了什么」，天然本地，无需同步。
- 推送触发条件统一为「本地状态机 apply 了一条变更」，两个来源：owner 的本地写、副本的复制 apply。
- 推送只带 key + version，客户端从当前连接节点拉内容（本地读）。
- 不采用「订阅集中到 owner、owner 统一推」：那需要订阅/退订/连接清理全部跨节点同步，owner 成推送单点。
- 重复投递兜底（沿用 M0）：per-key version 单调守卫 + 对账只返回有差异的键。

## 6. 校验/反熵

```
每 5 秒（可配）：
  副本 ──我持有的分区摘要 {key→version 全量表}──▶ 该分区 owner
  owner 比对：
     我有对方没有的 / 版本更新的 ──DISTRO_PULL──▶ 对方增量拉取
     对方有我没有的 → 异常，报警
```

- 摘要粒度：先比 key→version 表哈希，不一致再逐 key 增量拉取。
- 比对表**含墓碑**，否则「我删了你没删」识别不出。
- 方向固定为副本主动向 owner（owner 是真相源，单向简单）；不采用 owner 巡检所有副本。
- 因为每个节点持有全量数据，校验循环天然每周期覆盖全体成员——它同时就是探活（见 §7）。
- 平时一轮校验只有几百字节往返，哈希相等即空转。

## 7. 故障检测与场景推演

**不建独立探活系统。** v1 故障检测不驱动任何正确性决策（owner 不迁移、数据不搬家），判断只影响状态页显示与错误提示，判断错了代价为零。

检测寄生在三类已有交互上：

| 接触面 | 频率 | 失败时 |
|---|---|---|
| 写转发 | 随写请求 | 标记 owner 不可达，报「owner 不可用」 |
| 异步复制 SYNC | 随写请求 | 标记副本不可达（owner 不在乎，校验会补） |
| 周期校验 | 每 5s | 标记不可达 |

健康判定：`healthy = 最近一次校验/转发成功距今 < N 个周期`（如 15s = 3 周期）。客户端侧用自己的请求超时换节点（M0 机制），不依赖服务端互探。

过渡期补充（2026-11-01）：AP-2/3 的转发与复制交互尚未接入时，校验也不存在，
peer 健康位没有任何数据源。为避免控制台长期显示"2/3 异常"的误导，AP-1 阶段
加入**纯展示探测**：每 5s 对各 peer 的 remoting 端口做一次 TCP connect（1s 超时），
15s 健康窗口。它不参与任何协议决策（写转发/复制/校验各自按自己的交互判断对端），
AP-4 校验接入后由"最近一次校验成功时间"点亮健康位，该探测线程移除。

**挂普通副本**（设 n3 为 test/* owner，同时持有全量数据）：

| 时刻 | 行为 |
|---|---|
| 瞬间 | 连 n3 的客户端故障转移（M0） |
| ≤5s | 各节点标记 n3 不可达；n3 的 test/* 数据停止更新（stale，读推送照常） |
| 期间 | prod/* 不受影响；test/* 写不可用 |
| 重启 | 本地恢复 → 校验发现差异 → 向 owner 增量拉齐收敛 |

**挂 owner**（设 n2 为 prod/* owner）：

| 时刻 | 行为 |
|---|---|
| 瞬间 | prod/* 写转发失败，明确报「owner 不可用」；**读/推送不受影响**，数据停在最后同步点 |
| ≤5s | 各节点标记 n2 不可达 |
| 期间 | prod/* 无写，副本差异不增长 |
| 重启 | 本地日志+快照恢复（已应答写一条不少）→ 继续当 owner → 副本补「已应答但 SYNC 未发出」的尾巴 |

不对称性：挂 owner 损失写可用性但不丢数据、差异不增长；挂副本损失新鲜度，差异持续积累、重启时一次性补。

## 8. 版本、删除与元数据

| 项 | 决策 |
|---|---|
| version | per-key，owner 分配，**跨节点权威单调序号**。客户端守卫、PushEvent、BATCH_SUBSCRIBE 对账均按 key 比 version |
| revision | 降级为**节点本地日志/快照位点序号**，各节点不同，仅本地有意义 |
| 为什么不能全局 revision | 分区各有 owner，多个 owner 的序号无法排成全集群单调序列；全局单序需要单写者，那是 CP |
| 删除 | 带 version 的墓碑 mutation，走同一复制链路 |
| 墓碑 GC | v1 不 GC，与 history 截断策略对齐 |
| workspace 元数据 | `createNamespace/createGroup`（含空 ns/group）视为内置分区（如 `ns=_system, group=_workspaces`）走同一套复制，不单独设计机制 |

## 9. 内部协议

- 复用现有 remoting 端口，不另开内部端口；ISSUE-19 鉴权时再评估拆分与双向认证。
- 新增 3xxx 段协议码：

| 协议码 | 用途 |
|---|---|
| DISTRO_FORWARD | 写转发（任意节点 → owner） |
| DISTRO_SYNC | 增量复制批次（owner → 副本） |
| DISTRO_VERIFY | 副本向 owner 送分区摘要 |
| DISTRO_PULL | 差异/增量拉取（owner → 副本，应答 VERIFY 或按需） |

## 10. 与 LocalProtocol 的关系（工程建议）

- 抽取 `LocalEngine`（日志 + 状态机 + 组提交 + 快照），`LocalProtocol` 与 `DistroProtocol` 都委托，避免两份 600 行代码各自演化。（✅ 2026-11-01 落地，见 AP-2 实施记录）
- 引擎两个入口：
  - `proposeLocal`：owner 路径——校验 expectedVersion、分配 version、落盘（≈ 现 propose）。
  - `applyReplicated`：副本路径——version 用复制批次携带的值，不自行分配，仍走本地日志+快照（重启不依赖 peers 在线）。
- `LocalProtocol` 对外行为不变。

## 11. 里程碑（每个可独立验收）

| 里程碑 | 内容 | 验收 |
|---|---|---|
| AP-1（✅ 2026-11-01） | 成员配置（`--cluster-members`）+ DistroProtocol 骨架（先委托本地引擎），mode=ap 可启动 | 3 节点独立工作，状态页正确 |
| AP-2（✅ 2026-11-01） | 写转发：任意节点 publish → owner 生效；转发节点同步 apply（写后即读）；expectedVersion 语义保持 | 409 冲突照常；发布后原节点立即可读 |
| AP-3 | 异步复制 + 副本本地读/推送 | 写 owner，连非 owner 的客户端收到推送 |
| AP-4 | 校验/反熵 + 重启收敛 | 杀副本/杀 owner/滚动重启后数据收敛 |
| AP-5 | 集群集成测试矩阵 + 控制台集群页（每节点 ownedPartitions、复制滞后、校验时间、健康） | 完整演示 |

验收总目标：3 节点 loopback，写任意节点 → 全集群可读可推送；owner 宕机重启后数据完整。

### AP-2 实施记录（2026-11-01，已完成）

- **LocalEngine 抽取**（§10 落地）：`core/engine/LocalEngine` 承载日志 + 状态机 + 组提交 +
  快照，两个写入口——`proposeLocal`（本地/owner：校验 expectedVersion、分配版本、落盘）、
  `applyReplicated`（副本：使用命令携带的 `assignedVersion`，同样落本地日志并等待刷盘）。
  `LocalProtocol` 与 `DistroProtocol` 均委托引擎，不再各自持有存储。
- **版本语义迁移**（§8 落地）：
  - 状态机 `apply(command, revision, version)`：本地路径 version=revision（standalone 与
    owner 行为不变）；副本路径 version=owner 分配值，与节点本地 revision 解耦。
  - `ConfigCommand.assignedVersion` 参与 `CommandCodec` 编码（JSON 可选字段），
    重启回放后版本与 owner 一致；旧日志缺该字段按本地分配回退，向后兼容。
  - `expectedVersion` 改与 per-key `version` 比较（原为 revision）——跨节点语义唯一正确。
  - 订阅/对账声明字段 `revision` 更名 `version`（`SubscribeRequest`/`SubscribeItem`），
    服务端 `isUnchanged` 比较 `entry.getVersion()`，md5 兜底保证新旧协议互通。
  - 客户端单调守卫（ISSUE-02）从 revision 切换为 version（`versionCache`），
    推送删除守卫同步用 `PushEvent.version`。
- **写转发**（3xxx 内部协议，复用 remoting 端口）：
  - `DISTRO_FORWARD`(3001)：任意节点 → owner；owner 侧 `DistroForwardProcessor`
    直接 `proposeLocal`（不经 ConfigService，避免二次路由），应答携带 owner 分配的
    version 与命令回显。
  - 转发节点收到应答后 `applyReplicated` 同步 apply（写后即读），再应答 HTTP 调用方；
    已应答的写在转发节点同样持久化。
  - owner 不可达 → `UNAVAILABLE`（HTTP 503），错误信息带 owner 节点 ID；
    版本冲突 → 409，NOT_FOUND → 404。
  - `DISTRO_READ`(3002)：`LINEARIZABLE` 读转发 owner；`ConfigService.get` 默认改
    `LOCAL`（§4 控制台/SDK 路径）。
- 读语义：默认 LOCAL；LINEARIZABLE 转发 owner 已实现，当前无默认调用方。
- workspace 元数据仍为各节点本地登记，AP-3 随复制一起集群化（§8 既定计划）。
- 测试：`ApForwardIntegrationTest`（2：非 owner 发布→owner 生效+转发节点写后即读+
  跨节点 409/200 乐观锁；owner 宕机→503）；`DistroProtocolTest` 改为写本节点拥有的
  分区；`ApClusterIntegrationTest` 改单成员集群；客户端 ISSUE-02 测试改按 version 断言。

### AP-1 实施记录（2026-11-01，已完成）

- `--cluster-members`：格式 `nodeId=host:httpPort:remotingPort`，逗号分隔。`consistency/ClusterMember`
  负责解析与校验（格式、端口范围 1..65535、节点 ID 去重）；AP 模式必须提供成员列表且包含本节点
  `--node-id`，否则启动拒绝。
- `--mode`：可选 `standalone / cp / ap`（大小写不敏感，build 时校验）；`cp` 在装配时明确报
  「由 Raft 实现，尚未提供（M1 里程碑）」。
- `core/distro` 新增：
  - `DistroAssignments`：分区（ns+group）→ owner，`floorMod(hash(ns + ' ' + group), 成员数)`，
    成员按 nodeId 排序保证各节点算出同一张表；`String.hashCode()` 由语言规范固定，跨 JVM 稳定。
  - `DistroProtocol` 骨架：存储与单机链路完全委托 `LocalProtocol`（LocalEngine 抽取推迟到 AP-2，
    届时与「转发节点先 apply 再应答」一并做）；状态上报 `ProtocolMode.AP`。
- `NodeRole.PEER`：AP 节点级角色——全量副本下每个节点既是若干分区的 owner 又是其余分区的副本，
  单一角色无法用 OWNER/REPLICA 表达，统一报 PEER；分区级归属由 `DistroAssignments` 查询。
- peer 健康位：AP-1 尚无节点间交互，初始如实报 `healthy=false`（语义为「未知」）；
  2026-11-01 补充纯展示探测（5s TCP connect + 15s 健康窗口，见 §7 过渡期补充），
  AP-3/AP-4 接入 SYNC/VERIFY 后改由最近一次成功的复制/校验点亮并移除该探测。
- 订阅观测（2026-11-01 补充，M0 ISSUE-16 的一部分）：`SubscriptionRegistry.snapshot()`
  暴露「连接 → 订阅键列表」快照；新增 `GET /v1/cs/subscriptions`（total + 每连接
  remote/count/keys）；控制台集群页新增订阅者面板。订阅表是节点本地的，
  每个节点只显示连着自己的客户端。
- `ConfigProtocol` 增加 `setNodeAddress`（HTTP/remoting 绑定完成后上报对外地址，CP 也复用）。
- AP-1 阶段各节点独立接受本地写（行为等价 standalone），写转发在 AP-2 接入。
- 测试：`ServerOptionsTest`（+7：成员解析/非法格式/重复 ID/AP 缺成员/缺本节点/未知模式/默认值）、
  `DistroAssignmentsTest`（6：确定性/全成员覆盖/非法成员表）、`DistroProtocolTest`（3：委托链路
  + AP 状态上报、重启恢复、成员表校验）、`ApClusterIntegrationTest`（2：mode=ap 端到端发布读取、
  409 乐观锁、重启恢复；CP 模式明确拒绝）。

## 12. 决策记录

| 日期 | 决策 |
|---|---|
| 2026-11-01 | 副本模型定为全量副本 + 分片写责任（分片只切写责任，不切数据） |
| 2026-11-01 | 版本语义：per-key version 跨节点权威，revision 降级为节点本地序号 |
| 2026-11-01 | 写后即读：转发节点收到 owner 应答后先本地 apply 再回复客户端 |
| 2026-11-01 | 默认读一致性 LOCAL；LINEARIZABLE 转发 owner |
| 2026-11-01 | 校验方向为副本主动向 owner；不建独立探活，检测寄生在转发/复制/校验上 |
| 2026-11-01 | **不做 owner 故障转移**，owner 宕机期间该分区写不可用；转移/选举归 CP（Raft）里程碑 |
| 2026-11-01 | 内部协议复用 remoting 端口 + 3xxx 协议码 |
| 2026-11-01 | workspace 元数据作为内置分区走同一复制链路 |
| 2026-11-01 | 删除为带 version 的墓碑，v1 不 GC |
| 2026-11-01 | 工程上抽 LocalEngine 复用 LocalProtocol 引擎（待实施时最终确认） |
| 2026-11-01 | LocalEngine 落地：proposeLocal / applyReplicated 两入口，expectedVersion 改比 per-key version |
| 2026-11-01 | 订阅/对账与客户端守卫的比较基准由 revision 切换为 version（wire 字段更名，md5 兜底兼容） |
| 2026-11-01 | ConfigService 默认读改 LOCAL；LINEARIZABLE 转发 owner 已实现但无默认调用方 |
