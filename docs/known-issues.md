# poly-config 已知问题清单

> 范围：M0 单机核心链路
> 更新：2026-10-31（逐项对照代码核实）
> 状态：已核实——ISSUE-01、04 已修复；其余未修复；修复顺序见下节

## 优先级

| 级别 | 含义 |
|---|---|
| P0 | 正确性、数据安全或默认安全问题，优先处理 |
| P1 | 性能、可靠性、规模问题，M0 之后尽快处理 |
| P2 | 安全加固、产品化、可观测性，后续迭代 |

## 总览

| ID | 优先级 | 类别 | 问题 | 影响 |
|---|---|---|---|---|
| ISSUE-01 | P0 | 存储 | SegmentLog 缺少数据目录文件锁（已修复，2026-10-31） | 同一目录双开会写坏日志 |
| ISSUE-02 | P0 | 客户端 | 推送/对账响应乱序时配置可能回退 | 旧内容覆盖新内容 |
| ISSUE-03 | P0 | 客户端 | 同一配置的监听回调可能乱序 | 监听器看到旧值晚于新值 |
| ISSUE-04 | P0 | 存储 | 恢复截断静默删除后续段（已修复，2026-10-31） | 数据丢失不可审计 |
| ISSUE-05 | P0 | 安全 | 默认监听 0.0.0.0 且无鉴权（remoting 端口无视 --host） | 局域网内可读写 |
| ISSUE-06 | P1 | 存储 | 无快照与日志前缀压缩 | 日志无限增长、重启变慢 |
| ISSUE-07 | P1 | 性能 | 每条命令 fsync，无 group commit | 写吞吐受磁盘延迟限制 |
| ISSUE-08 | P1 | 推送 | 无 UNSUBSCRIBE，移除监听后仍推送 | 无效推送与资源浪费 |
| ISSUE-09 | P1 | 推送 | 单连接订阅数无上限 | 推送遍历与内存膨胀 |
| ISSUE-10 | P1 | 客户端 | 定时对账逐 key 同步请求 | 订阅多时对账很慢 |
| ISSUE-11 | P1 | 客户端 | addListener 同步初始订阅可能阻塞 | 服务端不可用时调用方卡住 |
| ISSUE-12 | P1 | 核心 | 历史只存在内存、按条数截断 | 不能长期审计 |
| ISSUE-13 | P1 | 接口 | HTTP 请求体无大小限制 | 内存压力与 DoS 风险 |
| ISSUE-14 | P1 | 接口 | list / history 无分页 | 大数据量响应过大 |
| ISSUE-15 | P1 | 核心 | 无并发写冲突检测 | 后写静默覆盖先写 |
| ISSUE-16 | P1 | 运维 | 可观测性不足 | 故障定位困难 |
| ISSUE-17 | P1 | 存储 | SegmentLog 全局同步，scan 阻塞写 | 大日志扫描阻塞写入 |
| ISSUE-18 | P1 | 运维 | 关闭流程不等待 graceful shutdown | 在途请求失败、端口释放慢 |
| ISSUE-19 | P2 | 安全 | 无鉴权、TLS、限流 | 任意客户端可读写 |
| ISSUE-20 | P2 | 安全 | 配置与客户端快照明文落盘 | 敏感配置泄露 |
| ISSUE-21 | P2 | 审计 | 无身份与审计日志 | 变更无法追责 |
| ISSUE-22 | P2 | 兼容 | 无协议/存储格式版本策略 | 升级兼容风险 |
| ISSUE-23 | P2 | 易用性 | CLI 参数解析异常不友好 | 启动失败信息不明确 |
| ISSUE-24 | P2 | 测试 | core 测试缺少 SLF4J 绑定 | 测试日志告警 |
| ISSUE-25 | P2 | 控制台 | 无登录（分页、搜索已实现） | 控制台无访问控制 |
| ISSUE-26 | P2 | 运维 | Windows 控制台中文日志乱码 | 日志可读性差 |

## 修复顺序（2026-10-31 核实后，围绕单机核心链路）

> 单机核心链路 = 写入（客户端 → HTTP/remoting → propose → SegmentLog）+ 订阅/推送 + 重启恢复。
> 核实结论：ISSUE-01、ISSUE-04 已修复且有测试覆盖；其余 24 项未修复（其中 ISSUE-11、15、25
> 有部分进展，已并入对应条目）。ISSUE-05 经核实 remoting 端口无视 `--host`，修复范围扩大。

| 梯队 | 顺序 | 条目 | 理由 |
|---|---|---|---|
| T1 正确性/安全 | 1→4 | ISSUE-02 → 05 → 03 → 15 | 配置回退、默认暴露（需连带修 remoting 绑定）、回调乱序；ISSUE-15 骨架已在，成本最低 |
| T2 性能/可靠性 | 5→8 | ISSUE-07 → 06 → 17 → 18 | group commit 提吞吐；快照的隔离前置（ISSUE-04）已就位；锁分离为快照扫描铺路 |
| T3 订阅/推送 | 9→13 | ISSUE-10 → 08 → 11 → 09 → 13 | ISSUE-13 体量小，可顺手提前 |
| T4 外围 | — | 其余（12/14/16/19-26） | 非核心链路，暂缓 |

## 详细说明

### ISSUE-01 SegmentLog 缺少数据目录文件锁（已修复，2026-10-31）

- 位置：`storage/log/SegmentLog`
- 现状：打开日志时只创建目录和文件通道，没有进程级互斥。
- 风险：同一数据目录启动两个 server 会同时追加和截断日志，造成数据损坏。
- 建议：在数据目录抢 `.store.lock` 的 `FileLock`，失败直接拒绝启动；关闭时释放。
- 验证：同一目录连续打开两个 `SegmentLog`，第二个必须抛异常；第一个关闭后第二个可以打开。
- 修复：构造时对 `<baseDir>/.store.lock` 抢独占 `FileLock`（含同进程重复打开的
  `OverlappingFileLockException` 处理），抢不到抛 `STORAGE_ERROR` 拒绝启动；
  `close()` 时释放；恢复阶段抛异常时也释放锁。测试见
  `SegmentLogTest.shouldRejectSecondOpenOnSameDir`。

### ISSUE-02 推送/对账响应乱序时配置可能回退

- 位置：`poly-config-client` 的 `PolyConfigClient.applyView`
- 现状：每次拉取成功都无条件覆盖 `contentCache` 和 `md5Cache`，没有比较版本。
- 风险：同一 key 连续两次变更，两个拉取并发执行，较旧 revision 的响应后到，会把新内容覆盖回旧内容。
- 建议：为每个 key 记录已应用的 revision，只允许 `view.revision >= lastRevision` 覆盖；推送和对账共用同一规则。
- 验证：构造两个延迟不同的拉取响应，确认旧 revision 不会覆盖新 revision。

### ISSUE-03 同一配置的监听回调可能乱序

- 位置：`PolyConfigClient.notifyListeners`
- 现状：回调统一提交到 4 线程的 `callbackExecutor`，同一 key 的多次回调可能并发执行。
- 风险：监听器先收到新值、后收到旧值，业务侧基于回调做本地更新时出现回退。
- 建议：按 key 串行回调，或给回调带上 revision 让调用方过滤；更简单的是每 key 一个串行队列。
- 验证：同一 key 快速触发两次变更，断言回调顺序与 revision 一致。

### ISSUE-04 恢复截断静默删除后续段（已修复，2026-10-31）

- 位置：`SegmentLog.recover`、`deleteFrom`、`deleteQuietly`
- 现状：发现损坏或文件缺口后直接删除后续段，删除失败也被吞掉；只暴露一个 `recoveryTruncatedOffset`。
- 风险：数据丢失缺少审计线索，排障时无法知道丢了哪些段、为什么丢。
- 建议：删除前把段文件加入 `.quarantined` 清单；记录截断 offset、删除文件数和原因；在集群状态或日志中暴露。
- 验证：制造中间段损坏，重启后检查隔离清单、状态字段和日志。
- 修复：恢复阶段不再删除段文件，改为移动到 `<baseDir>/.quarantined/<原名>.<时间戳>`
  并记录 WARN 日志（含原因）；移动失败时保留原文件、下次启动重试。
  新增 `getQuarantinedFiles()` 暴露隔离清单，`LocalProtocol` 启动日志一并输出。
  隔离文件名不满足段文件命名规则，不会被再次扫入。测试见
  `SegmentLogTest.shouldQuarantineSubsequentSegmentsOnGap`。
  注意：`truncateTo` 是调用方主动截断的 API，其删除行为保持不变。

### ISSUE-05 默认监听 0.0.0.0 且无鉴权（核实：remoting 无视 --host，范围比原描述大）

- 位置：`ServerOptions` 默认 `host=0.0.0.0`，HTTP/remoting 均无鉴权
- 现状：开发默认配置会把管理面和长连接暴露到所有网卡，任何能访问端口的客户端都可以发布、删除、订阅。
  核实（2026-10-31）：`--host` 只作用于 HTTP（`PolyConfigServer` 用 `options.getHost()` 绑 8848）；
  remoting 构造 `NettyRemotingServer` 时只传端口，Netty `localAddress(new InetSocketAddress(port))`
  永远绑通配地址（9848）——显式 `--host=127.0.0.1` 也拦不住长连接端口。
- 风险：局域网或误暴露到公网时，配置可被任意读写。
- 建议：默认绑定 `127.0.0.1`，**并同时把 host 传入 NettyRemotingServer（remoting-core 构造器需增加
  bind 地址参数）**；集群模式再显式配置 `0.0.0.0` 并配合防火墙。只改 `ServerOptions` 默认值只能堵住 HTTP。
- 验证：默认启动后 HTTP 与 remoting **都**只监听回环；显式传 `--host=0.0.0.0` 时两者都对外。

### ISSUE-06 无快照与日志前缀压缩

- 位置：`poly-config-core` 的 `LocalProtocol` 构造；`poly-config-storage`
- 现状：启动时 `SegmentLog.scan` 全量回放；存储层没有快照，也没有删除前缀段的能力。
- 风险：日志持续增长，重启时间线性增加；历史数据无法按快照压缩。
- 建议：增加 `SnapshotStore`（原子快照 + 版本号）和日志前缀压缩/epoch 目录；启动时加载最新快照再回放增量。
- 验证：写入大量命令后重启，比较回放条数与启动耗时；快照后旧段可删除。

### ISSUE-07 每条命令 fsync，无 group commit

- 位置：`LocalProtocol.propose`
- 现状：每次 `append` 后立即 `flush`，一条命令一次 fsync。
- 风险：写吞吐受磁盘 fsync 延迟限制，发布频率高时延迟明显。
- 建议：实现 group commit：批量追加，统一 force 后再批量应答；必须保证应答前已持久化。
- 验证：压测不同批量大小下的写吞吐和 p99 延迟。

### ISSUE-08 无 UNSUBSCRIBE，移除监听后仍推送

- 位置：`SubscriptionRegistry`、`ChangePusher`、`PolyConfigClient.removeListener`
- 现状：客户端移除最后一个监听器后只清理本地状态，服务端仍然保留订阅并继续推送；客户端收到推送后还会拉取内容。
- 风险：无效网络流量、服务端订阅表膨胀、客户端缓存做无意义更新。
- 建议：增加 `UNSUBSCRIBE` 协议码，服务端精确移除 channel→key；客户端在没有监听器时发送。
- 验证：订阅后移除监听，触发变更，确认服务端不再推送。

### ISSUE-09 单连接订阅数无上限

- 位置：`SubscriptionRegistry.subscribe`
- 现状：任意连接可以无限添加订阅键。
- 风险：错误客户端或恶意客户端造成服务端内存增长，变更时遍历大量订阅。
- 建议：配置每连接最大订阅数，超出返回错误；记录订阅总数指标。
- 验证：超过上限订阅时收到明确错误，服务端内存稳定。

### ISSUE-10 定时对账逐 key 同步请求

- 位置：`PolyConfigClient.resyncSafely`
- 现状：每 30 秒对每个订阅 key 发一次同步 `invokeSync`。
- 风险：订阅多时对账耗时长；服务端故障时每个 key 都要重试所有节点。
- 建议：批量订阅接口、按 revision 增量拉取、首次失败后短路并退避。
- 验证：模拟 1000 个订阅和服务端离线，测量对账耗时与请求数。

### ISSUE-11 addListener 同步初始订阅可能阻塞

- 位置：`PolyConfigClient.addListener` → `subscribe`
- 现状：添加监听器时同步发订阅请求，服务端不可用会按超时逐个尝试地址。
  已缓解（2026-10-31 核实）：初始订阅失败不再抛出，仅 WARN 并等定时对账补救；
  但同步等待超时未变，服务端不可用时注册监听仍会阻塞至超时。
- 风险：业务启动阶段注册多个监听器时被阻塞。
- 建议：初始订阅改为异步，先读本地快照，后台重试；或提供同步/异步两种 API。
- 验证：服务端离线时 `addListener` 立即返回，监听器在未来恢复后仍能收到变更。

### ISSUE-12 历史只存在内存、按条数截断

- 位置：`ConfigStateMachine.histories`
- 现状：每个 key 保留最近 `historyLimit` 条历史，重启通过日志回放重建。
- 风险：不能长期审计；历史策略与日志保留策略不一致。
- 建议：历史独立落盘或纳入快照；保留策略按时间/版本可配置；API 分页读取。
- 验证：写入超过 `historyLimit` 次变更，确认历史策略和重启后的可见范围符合预期。

### ISSUE-13 HTTP 请求体无大小限制

- 位置：`ApiHandler.readBody`
- 现状：一次性读入完整请求体，没有上限；remoting 解码有 16MB 限制，HTTP 没有对应限制。
- 风险：超大配置内容造成堆内存压力和 DoS。
- 建议：增加最大请求体配置（例如 1MB），超限返回 413；与单条配置大小上限保持一致。
- 验证：发送超过上限的请求，确认返回 413 且服务端内存稳定。

### ISSUE-14 list / history 无分页

- 位置：`ApiHandler.handleListConfigs`、`handleHistory`、`ConfigStateMachine.list`
- 现状：一次返回 namespace/group 下全部配置或全部历史。
- 风险：配置数量大时响应体大、序列化慢、控制台卡顿。
- 建议：增加 `page`/`size` 或游标分页；history 已有 limit，补充偏移/游标。
- 验证：大量配置下分页读取，响应大小稳定。

### ISSUE-15 无并发写冲突检测

- 位置：`ConfigService.publish`、`ConfigCommand`
- 现状：发布总是覆盖当前值，没有 `expectedVersion`。
  部分进展（2026-10-31 核实）：`VERSION_CONFLICT(40901)` 错误码与 HTTP 409 映射已存在
  （`ErrorCode`、`ApiHandler`），只缺 `ConfigCommand.expectedVersion` 字段与状态机校验。
- 风险：两个控制台同时编辑，后提交者静默覆盖前者。
- 建议：发布请求支持可选 `expectedVersion`，状态机校验不匹配返回 `VERSION_CONFLICT`。
- 验证：两个客户端基于同一版本发布，第二个收到 409。

### ISSUE-16 可观测性不足

- 位置：`NodeStatus`、服务端各处
- 现状：状态只有节点、角色、健康、任期等基本字段；没有写延迟、推送失败数、订阅数、日志大小、回放耗时。
- 风险：线上问题只能看日志，无法快速定位。
- 建议：增加基础指标（计数器/直方图）和集群状态字段；后续接 Prometheus 或自研面板。
- 验证：压测和故障注入时能从指标看到变化。

### ISSUE-17 SegmentLog 全局同步，scan 阻塞写

- 位置：`SegmentLog` 的 `synchronized` 方法和 `scanFrom`
- 现状：追加、读取、扫描、截断共用同一个锁；扫描期间 visitor 执行也持锁。
- 风险：长扫描会阻塞写入；后续 Raft 快照扫描会更明显。
- 建议：读写分离或扫描时生成稳定快照后释放锁；协议层再配合批量读取。
- 验证：扫描大日志时并发写入，测量写延迟。

### ISSUE-18 关闭流程不等待 graceful shutdown

- 位置：`PolyConfigServer.stop`、`PolyConfigClient.close`
- 现状：`httpServer.stop(0)` 立即停止，`remotingServer.shutdown()` 不等待 event loop 结束；客户端也不等待。
- 风险：在途请求被中断，测试中端口释放有时序问题。
- 建议：先停止接收新请求，再等待在途请求和 Netty 线程池退出，设置超时兜底。
- 验证：关闭时并发请求，确认要么成功要么得到明确失败，端口及时释放。

### ISSUE-19 无鉴权、TLS、限流

- 位置：`ApiHandler`、`SubscribeProcessor`、`FetchProcessor`
- 现状：管理面和长连接均无需认证；HTTP 是明文；没有请求频率限制。
- 风险：任意客户端可读写配置；传输和存储内容可被窃听。
- 建议：token 或 RBAC、TLS、连接/请求限流；先做最小 token，再扩展。
- 验证：未携带 token 的请求被拒绝；TLS 握手成功；限流生效。

### ISSUE-20 配置与客户端快照明文落盘

- 位置：`LocalProtocol` 的日志、`PolyConfigClient.writeSnapshot`
- 现状：配置内容和客户端快照都是明文。
- 风险：磁盘上的敏感配置（数据库密码等）可被读取。
- 建议：存储加密或至少快照文件权限控制；后续做配置加密插件。
- 验证：检查数据目录和快照文件的权限与内容。

### ISSUE-21 无身份与审计日志

- 位置：`HistoryEntry.operator` 只是调用方自报
- 现状：没有认证身份，操作人字段可伪造；没有独立审计日志。
- 风险：变更无法追责。
- 建议：鉴权后从 token 提取主体，写入历史；增加审计日志（时间、主体、IP、操作、结果）。
- 验证：伪造 operator 无效；审计日志可查询。

### ISSUE-22 无协议/存储格式版本策略

- 位置：`ProtocolCodes`、`SegmentLog` 帧版本、客户端/服务端协议
- 现状：帧有 version 字节，但协议码和存储格式没有升级/兼容策略。
- 风险：后续增加 UNSUBSCRIBE、Raft 日志字段时，新旧客户端/数据目录不兼容。
- 建议：定义协议版本协商、存储格式迁移和回滚方案；在启动时校验。
- 验证：旧客户端连接新服务端的兼容矩阵测试。

### ISSUE-23 CLI 参数解析异常不友好

- 位置：`ServerOptions.fromArgs`
- 现状：数字参数直接 `Integer.parseInt`，非法值抛 `NumberFormatException`，缺少参数名上下文。
- 风险：启动失败信息不明确。
- 建议：包装为 `PolyConfigException`，提示参数名和期望格式。
- 验证：传入 `--http-port=abc` 得到清晰错误。

### ISSUE-24 core 测试缺少 SLF4J 绑定

- 位置：`poly-config-core` 测试
- 现状：有 `slf4j-api` 但没有测试期 binding，测试输出 NOP 警告。
- 风险：仅影响测试日志，不影响功能。
- 建议：给 core 加 test scope 的 logback，或父 POM 统一测试日志配置。
- 验证：测试时不再出现 StaticLoggerBinder 警告。

### ISSUE-25 控制台无分页、搜索、登录

- 位置：`poly-config-server/src/main/resources/web`
- 现状：控制台已实现前端搜索与分页（`web/app.js` 的 pagedConfigs/pageCount、搜索框）；
  仍缺登录/访问控制（无鉴权时控制台本身也没有访问控制）。
- 风险：配置多时不好用；无鉴权时控制台本身也没有访问控制。
- 建议：随鉴权和分页一起做；先加分页和关键字过滤。
- 验证：配置量大时控制台可翻页和搜索。

### ISSUE-26 Windows 控制台中文日志乱码

- 位置：`logback.xml` 与 Windows 控制台编码
- 现状：测试和运行日志里的中文在 GBK 终端显示为乱码。
- 风险：日志可读性差，不影响功能。
- 建议：logback 编码显式设置 UTF-8，并在文档中说明终端编码；或使用文件日志。
- 验证：Windows 终端和 IDE 控制台中文正常。
