# poly-config 产品形态

> 状态：产品形态与核心决策已确认，可以进入工程骨架
> 更新：2026-10-29

## 1. 一句话定位

独立部署的配置中心集群 + Java SDK + Web 控制台，支持 AP / CP 两种集群模式，部署时切换。

项目重点不在配置管理的 CRUD，而在分布式场景下的工程实现：集群复制、存储、推送、故障切换与可观测性。

## 2. 已确认的形态

| 维度 | 决策 |
|---|---|
| 项目名 | poly-config |
| 交付物 | server 集群 + Java SDK + Web 控制台 |
| 使用方式 | 独立部署，业务应用通过 SDK 接入 |
| 集群模式 | standalone / cp / ap，同一套代码，部署时切换 |
| 管理面 | HTTP OpenAPI：配置 CRUD、历史、回滚、集群状态 |
| 数据面 | 客户端与 server 建 Netty 长连接，服务端主动推送变更 |
| 数据模型 | namespace（环境/租户）→ group（应用）→ dataId（配置项） |
| 功能范围 | 发布、读取、监听、历史、回滚 |
| AP 路线 | Distro 式（分片 + 异步复制 + 周期校验） |
| Java 版本 | Java 8 |
| 存储 | 纯内嵌存储（WAL + 快照），不依赖外部数据库 |
| CP 日志 | 自研 RaftLog（尾部截断、快照压缩、term/index 元数据） |
| AP 日志 | 自研，与 CP 共用统一分段日志底座 |
| 控制台 | 由 server 提供静态资源，不单独起进程 |
| 集群成员 | 静态配置：节点列表写在各节点配置文件，变更需修改配置并滚动重启 |
| 学习重点 | 分布式协议与工程实现，不做论文对照和形式化验证 |

## 3. 当前默认方案（待细化）

| 维度 | 方案 |
|---|---|
| 部署单位 | 单机 jar；集群 3 节点；附 docker-compose |
| 控制台 | 配置列表、编辑发布、历史版本、回滚、集群状态 |
| 客户端能力 | 读取、监听回调、本地快照、断线重连、故障转移 |
| 技术栈 | Java 8 + Maven 多模块，不引入 Spring；Netty 复用 netty-remoting-framework |

## 4. 使用路径

1. 启动 server：单机 `standalone`，或 3 节点 `cp` / `ap`
2. 在控制台创建 namespace / group / dataId，编辑并发布配置
3. 业务应用引入 SDK，读取配置并注册监听
4. 配置变更后，server 主动推送，客户端触发回调
5. server 不可用时，客户端读取本地快照；恢复后自动对账
6. 在控制台查看历史版本并执行回滚

## 5. 集群模式语义

| 模式 | 语义 |
|---|---|
| standalone | 单节点，内嵌存储，无复制 |
| cp | 手写 Raft：leader 写、多数派提交、快照、线性读（成员变更为后续里程碑） |
| ap | Distro 式：分片 + 副本 + 异步复制 + 周期校验/反熵；任意节点可写，服务端内部转发给 owner |

成员管理：v1 采用静态配置，节点列表写在各节点配置文件，变更需要修改配置并滚动重启；动态成员（Raft 单节点变更、Distro owner 重平衡）作为后续里程碑。

## 6. 存储方案

原则：内嵌、无外部依赖；日志是真相源；状态机放内存，快照负责恢复与压缩。

| 部分 | 方案 |
|---|---|
| 日志底座 | 自研统一分段日志：追加、CRC、滚动、尾部截断、刷盘 |
| CP 日志 | 在日志底座上实现 `RaftLog`：term/index、快照 + epoch 前缀压缩、term/vote/commit 元数据 |
| AP 日志 | 在日志底座上实现本地 mutation log：版本号、历史、按 key 重建 |
| 状态机 | 自研：内存 `ConcurrentSkipListMap` + 原子快照文件；CP 由 Raft 日志回放，AP 由本地日志回放 |
| 恢复校验 | 恢复截断点低于已持久化的 `commitIndex` 时拒绝启动 |

CP 与 AP 都不依赖 `mini-file-storage` 的 `FileStore` 门面。Raft 需要尾部截断、前缀压缩和 term/index 一等语义，门面是"连续只追加"模型，契约不一致；可以参考它的分段文件、帧格式和恢复思路，但日志接口自行定义。

## 7. v1 功能边界

做：

- 配置发布、读取（列表、删除作为 CRUD 基础操作）
- 客户端监听与服务端推送
- 历史版本与回滚
- 集群状态页（节点角色、复制状态、健康检查）
- HTTP OpenAPI（配置 CRUD、历史、回滚、集群状态）

不做（v1）：

- 服务发现
- 多语言 SDK
- RBAC、多租户、审计
- 灰度发布、配置加密
- 基础鉴权（后续再加）
- K8s Operator、Spring Cloud 集成

## 8. 模块骨架（提案）

```
poly-config
├── poly-config-common          数据模型、异常、常量
├── poly-config-storage         统一分段日志、状态机存储、快照
├── poly-config-consistency     一致性 SPI（StateMachine、Proposal、读语义）
├── poly-config-raft            CP 实现 + RaftLog
├── poly-config-distro          AP 实现 + 本地 mutation log
├── poly-config-core            配置服务、通知引擎、订阅注册表
├── poly-config-server          启动、HTTP API、集群装配、控制台资源
└── poly-config-client          SDK：缓存、监听、本地快照、故障转移
```

## 9. 待定项

暂无。核心决策已确认。

## 10. 决策记录

| 日期 | 决策 |
|---|---|
| 2026-10-29 | 项目名定为 poly-config |
| 2026-10-29 | 产品形态确定为独立部署 server 集群 + Java SDK + Web 控制台 |
| 2026-10-29 | 学习重点放在分布式协议的工程实现，不按论文/形式化验证推进 |
| 2026-10-29 | 存储采用纯内嵌方案，不依赖外部数据库 |
| 2026-10-29 | AP / CP 共用一套代码，部署时切换模式 |
| 2026-10-29 | 确认管理面为 HTTP OpenAPI（CRUD、历史、回滚、集群状态） |
| 2026-10-29 | 确认数据面为 Netty 长连接 + 服务端主动推送 |
| 2026-10-29 | 确认数据模型为 namespace → group → dataId |
| 2026-10-29 | 确认 v1 功能范围为发布、读取、监听、历史、回滚 |
| 2026-10-29 | AP 路线确定为 Distro 式 |
| 2026-10-29 | 项目使用 Java 8 |
| 2026-10-29 | CP 的 Raft 日志自行实现，不复用 FileStore 门面 |
| 2026-10-29 | AP 日志同样自研，与 CP 共用统一分段日志底座 |
| 2026-10-29 | 控制台由 server 提供静态资源，不单独起进程 |
| 2026-10-29 | 集群成员采用静态配置，动态成员列为后续里程碑 |
| 2026-10-29 | 控制台通过单节点的集群感知 API 管理整个集群，不直连各节点 |

## 11. 实施顺序（当前阶段）

M0 单机核心链路（当前）：

- common：配置模型、错误码、DTO
- storage：分段日志、恢复、截断、刷盘
- consistency：定义 SPI，暂不实现 Raft / Distro
- core：配置状态机、ConfigService、通知引擎
- server：启动装配、HTTP OpenAPI、静态控制台
- client：长连接、监听、本地快照、故障转移
- LocalProtocol：单机直接 apply，用来跑通链路

验收：HTTP 发布后客户端收到监听回调；重启不丢配置；服务端不可用时客户端读取本地快照。

M1 CP：Raft 与 RaftLog。

M2 AP：Distro 复制与分片。

## 12. 控制台与集群架构

控制台不是直连每台 server，而是由每个 server 以静态资源形式提供；浏览器只访问当前节点的同源 HTTP API。

```
浏览器控制台
  → 当前 server 的 HTTP API
  → ConfigProtocol
     → 写：CP 转发 leader / AP 转发 owner
     → 读：控制台使用 LINEARIZABLE，CP 走 leader 或 read-index，AP 读 owner
  → 集群状态：当前节点聚合本地状态与对等节点视图
```

约定：

- 打开任意节点的控制台都能管理整个集群，控制台不需要知道 leader/owner 地址
- 写请求由服务端内部转发，控制台保持无状态
- 节点故障时换一个节点打开控制台，或在前端挂 VIP / 负载均衡
- 控制台不做跨集群管理；多集群需要独立控制面，v1 不做
- 节点生命周期（动态加入/移除）不在 v1，静态成员变更需要滚动重启
