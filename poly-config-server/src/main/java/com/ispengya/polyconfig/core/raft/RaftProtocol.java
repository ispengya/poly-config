package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.codec.CommandCodec;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigConstants;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.ForwardWriteRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteResult;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.polyconfig.common.util.Preconditions;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.state.ConfigStateMachine;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CP（Raft）协议实现（cp-design.md）。
 *
 * <p>形态：单 leader 写、多数派提交、日志复制；读默认 LOCAL（任意节点读
 * 本地状态机），LINEARIZABLE 走 leader + read-index（CP-4）。写应答 =
 * commit + apply 之后（D7）。非 leader 收到写 → RAFT_FORWARD 转发 leader
 * （校验/赋版本都在 leader 侧状态线程完成），leader 应答携带 mutation +
 * index，转发节点按该 version 提前 apply（写后即读闭环），稍后 AppendEntries
 * 送达同一 index 时被幂等守卫跳过。</p>
 *
 * <p>D5 在途叠加水位：leader 的 propose 校验不只看已 apply 状态机，还叠加
 * "本 leader 已追加未提交"的在途条目（{@code pendingWrites}，index → key +
 * version）——并发写的 expectedVersion 判定与 AP 一气呵成语义一致，version
 * 分配永不碰撞；propose 完成（apply 或 step-down 失败）即移除对应水位，
 * step-down 时随在途请求一并失败。</p>
 *
 * <p>存储分工：日志走 {@link RaftLog}（raft/log，term/index 语义）、
 * term/vote 走 {@link RaftMetaStore}（raft/meta.json，先落盘再投票）、
 * 状态机复用 {@link ConfigStateMachine}（apply 即推送，与 AP 同一钩子）。
 * {@code LocalEngine} 的 propose 编排不复用——"追加即生效"与"commit 才
 * 生效"是两种编排（决策 D10/LocEngine 关系见设计文档 §10）。</p>
 *
 * <p>恢复（CP-2 范围，无快照）：单成员集群日志即权威，构造时把
 * commitIndex 置为 lastIndex 并立即 apply，重启后读路径无空窗；多成员
 * 为 0，等 leader 的 AppendEntries 重新提交（CP-3 起 leaderCommit 传播），
 * 此间 LOCAL 读为空窗、LINEARIZABLE 转发 leader 可读到已提交值。</p>
 */
public class RaftProtocol implements ConfigProtocol {

    private static final Logger log = LoggerFactory.getLogger(RaftProtocol.class);

    /**
     * 节点间 RPC 超时：随选举超时联动，测试可调小。
     */
    private static final long RPC_TIMEOUT_MIN_MILLIS = 200L;

    private static final long RPC_TIMEOUT_MAX_MILLIS = 3_000L;

    private final String nodeId;

    private final String clusterId;

    private final List<ClusterMember> members;

    private final ConfigStateMachine stateMachine;

    private final RaftLog raftLog;

    private final RaftMetaStore metaStore;

    private final NettyRemotingClient remotingClient;

    private final RemotingRaftTransport transport;

    private final RaftNode raftNode;

    /**
     * D5 在途叠加水位：本 leader 已追加未提交的写（index → key + version），
     * propose 校验时与已 apply 状态机叠加；propose 完成或 step-down 时移除。
     * 仅状态线程写入，完成回调可能在关闭线程触发——故用并发容器。
     */
    private final Map<Long, PendingWrite> pendingWrites = new ConcurrentHashMap<>();

    /**
     * 转发节点提前 apply 的 per-key 水位：已按 RAFT_FORWARD 应答落地的最大
     * version。同 key 更旧条目（含提前 apply 自身的重放）据此幂等跳过。
     */
    private final ConcurrentHashMap<ConfigKey, Long> earlyWatermark =
            new ConcurrentHashMap<>();

    /**
     * 日志路径连续 apply 到的位点（状态线程写、转发线程读）：≤ 本值的条目
     * 已按序走完日志 apply，转发应答无需再落地。
     */
    private volatile long logAppliedRevision;

    /**
     * 写提交超时：多数派不可用/选举中时在途写在该时限后以 UNAVAILABLE 失败
     * （条目可能随后提交——at-least-once，与 AP 转发超时同款语义）。
     */
    private final long proposeTimeoutMillis;

    /**
     * leader 侧转发等待：须大于本节点写提交超时（leader 自己的 propose 先超时
     * 并回包，避免转发方先断）。
     */
    private final long forwardTimeoutMillis;

    private volatile String host = "";

    private volatile int httpPort;

    private volatile int remotingPort;

    private volatile boolean closed;

    /**
     * 创建 CP 协议并完成恢复。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照阈值（CP-4 接入；CP-2 阶段忽略）
     * @param members 静态成员列表，不可为空且必须包含本节点
     * @param electionTimeoutMillis 选举超时基数（随机 [t, 2t)）
     * @param heartbeatMillis 心跳周期
     */
    public RaftProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit,
                        int snapshotThreshold, List<ClusterMember> members,
                        long electionTimeoutMillis, long heartbeatMillis) {
        if (members == null || members.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "CP 模式必须通过 --cluster-members 提供静态成员列表，"
                            + "格式 nodeId=host:httpPort:remotingPort，逗号分隔");
        }
        boolean selfIncluded = members.stream().anyMatch(m -> nodeId.equals(m.getNodeId()));
        if (!selfIncluded) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "CP 成员列表必须包含本节点 " + nodeId + "=host:httpPort:remotingPort");
        }
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        // Java 8 兼容：拷贝 + 不可变视图，防调用方构造后再改动成员表
        this.members = java.util.Collections.unmodifiableList(
                new ArrayList<>(members));
        this.stateMachine = new ConfigStateMachine(historyLimit);

        Path raftDir = dataDir.resolve("raft");
        this.metaStore = new RaftMetaStore(raftDir.resolve("meta.json"));
        this.raftLog = new RaftLog(raftDir.resolve("log"));

        this.remotingClient = new NettyRemotingClient();
        long rpcTimeout = Math.max(RPC_TIMEOUT_MIN_MILLIS,
                Math.min(electionTimeoutMillis, RPC_TIMEOUT_MAX_MILLIS));
        this.transport = new RemotingRaftTransport(remotingClient, rpcTimeout,
                Math.max(2, members.size()));

        // 恢复提交位点：单成员日志即权威（已应答写一条不少），多成员等 leader
        long recoveryCommit = members.size() == 1 ? raftLog.lastIndex() : 0L;
        this.raftNode = new RaftNode(nodeId, this.members, raftLog, metaStore, transport,
                this::onApply, electionTimeoutMillis, heartbeatMillis, recoveryCommit);
        this.proposeTimeoutMillis = Math.max(3_000L, 3 * electionTimeoutMillis);
        this.forwardTimeoutMillis = proposeTimeoutMillis + 1_000L;
        if (recoveryCommit > 0) {
            log.info("单成员恢复: commitIndex={}, 日志范围=[{},{}]", recoveryCommit,
                    raftLog.firstIndex(), raftLog.lastIndex());
        }
    }

    @Override
    public void start() {
        remotingClient.start();
        raftNode.start();
        log.info("RaftProtocol started, nodeId={}, clusterId={}, members={}, 选举超时可配",
                nodeId, clusterId, members);
    }

    @Override
    public void setNodeAddress(String host, int httpPort, int remotingPort) {
        this.host = host == null ? "" : host;
        this.httpPort = httpPort;
        this.remotingPort = remotingPort;
    }

    @Override
    public MutationResult propose(ConfigCommand command) {
        ensureOpen();
        if (raftNode.role() != RaftRole.LEADER) {
            return forwardWrite(command);
        }
        return proposeLocal(command);
    }

    /**
     * leader 本机写入口（RAFT_FORWARD 的 leader 侧也走这里）：D5 校验 +
     * 赋版本 + 追加 + 等待 commit+apply。
     *
     * <p>非 leader 调用由 {@code RaftNode.propose} 的角色闸拒绝（不二次转发）。</p>
     *
     * @param command 配置命令（携带 expectedVersion）
     * @return 提交结果
     */
    public MutationResult proposeLocal(ConfigCommand command) {
        ensureOpen();
        // D5：校验与赋版本在状态线程内按序执行，校验底账 = 已 apply + 在途叠加
        AtomicLong assignedIndex = new AtomicLong(0L);
        CompletableFuture<Long> applied = raftNode.propose(nextIndex -> {
            checkExpectedVersion(command);
            command.setAssignedVersion(nextIndex);
            byte[] payload = CommandCodec.encode(command);
            pendingWrites.put(nextIndex,
                    new PendingWrite(command.getKey(), nextIndex));
            assignedIndex.set(nextIndex);
            return payload;
        });
        applied.whenComplete((index, error) -> {
            // 完成即收编水位：成功 = 已并入状态机；失败 = step-down/关闭丢弃
            long cleanup = assignedIndex.get();
            if (cleanup > 0) {
                pendingWrites.remove(cleanup);
            }
        });
        long index = awaitIndex(applied, proposeTimeoutMillis);
        long version = command.getAssignedVersion() != null
                ? command.getAssignedVersion() : index;
        String md5 = command.getOperation() == ConfigOperation.DELETE
                ? "" : Md5Utils.md5Hex(command.getContent());
        return new MutationResult(command.getKey(), version, index, md5);
    }

    /**
     * 把写请求转发给 leader（RAFT_FORWARD，4004）。
     *
     * <p>leader 应答 = 条目已 commit+apply（D7），本节点按应答携带的
     * version 提前 apply（写后即读闭环），随后才应答调用方；稍后 AppendEntries
     * 送达同一 index 时被 {@link #applyIdempotent} 幂等跳过。leader 不可达/
     * 选举中 → {@code UNAVAILABLE}（HTTP 503）。</p>
     *
     * @param command 配置命令
     * @return 提交结果
     */
    private MutationResult forwardWrite(ConfigCommand command) {
        String leaderId = raftNode.leaderId();
        if (leaderId == null || leaderId.isEmpty()) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "集群选举中，暂无 leader，请稍后重试");
        }
        if (leaderId.equals(nodeId)) {
            // 视图滞后（实际已非 leader）：交回本地入口按状态线程的权威角色判定
            return proposeLocal(command);
        }
        ClusterMember leader = null;
        for (ClusterMember member : members) {
            if (member.getNodeId().equals(leaderId)) {
                leader = member;
                break;
            }
        }
        if (leader == null) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "leader 不在成员表中: " + leaderId);
        }
        return doForward(leader, command);
    }

    private MutationResult doForward(ClusterMember leader, ConfigCommand command) {
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.RAFT_FORWARD, null);
        request.setBody(JsonUtils.toJson(ForwardWriteRequest.from(command))
                .getBytes(StandardCharsets.UTF_8));
        RemotingCommand response;
        try {
            response = remotingClient.invokeSync(address(leader), request,
                    forwardTimeoutMillis);
        } catch (Exception e) {
            log.warn("写转发失败, leader={}, key={}: {}",
                    leader.getNodeId(), command.getKey().toPath(), e.toString());
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "leader 不可用: leader=" + leader.getNodeId()
                            + ", key=" + command.getKey(), e);
        }
        if (response.getCode() == ProtocolCodes.RESPONSE_VERSION_CONFLICT) {
            throw new PolyConfigException(ErrorCode.VERSION_CONFLICT,
                    describeResponse(response));
        }
        if (response.getCode() == ProtocolCodes.RESPONSE_NOT_FOUND) {
            throw new PolyConfigException(ErrorCode.NOT_FOUND, describeResponse(response));
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "写转发被 leader 拒绝: code=" + response.getCode()
                            + ", " + describeResponse(response));
        }
        ForwardWriteResult result = JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8),
                ForwardWriteResult.class);
        if (result == null || result.getDataId() == null) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "写转发应答体非法");
        }
        // 提前 apply：写后即读闭环（幂等守卫兜住稍后送达的同 index 条目）
        ConfigCommand applied = toCommand(result);
        applyForwarded(applied, result.getRevision(), result.getVersion());
        log.debug("写转发完成, leader={}, key={}, version={}",
                leader.getNodeId(), command.getKey().toPath(), result.getVersion());
        return new MutationResult(applied.getKey(), result.getVersion(),
                result.getRevision(), result.getMd5());
    }

    /**
     * 从转发应答还原携带 leader 分配版本的命令（revision = raft index）。
     */
    private static ConfigCommand toCommand(ForwardWriteResult result) {
        ConfigCommand command = new ConfigCommand(
                ConfigOperation.valueOf(result.getOperation()),
                new ConfigKey(result.getNamespace(), result.getGroup(),
                        result.getDataId()),
                result.getContent(),
                ConfigType.valueOf(result.getType()),
                result.getOperator(),
                result.getTimestamp());
        command.setAssignedVersion(result.getVersion());
        return command;
    }

    private static String address(ClusterMember member) {
        return member.getHost() + ":" + member.getRemotingPort();
    }

    private static String describeResponse(RemotingCommand response) {
        String remark = response.getRemark();
        if (remark != null && !remark.isEmpty()) {
            return remark;
        }
        if (response.getBody() != null && response.getBody().length > 0) {
            return new String(response.getBody(), StandardCharsets.UTF_8);
        }
        return "code=" + response.getCode();
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        ensureOpen();
        // CP-4：LINEARIZABLE 走 leader + read-index；CP-2 阶段两者同为本地读
        return stateMachine.get(key);
    }

    @Override
    public List<ConfigEntry> list(String namespace, String group) {
        ensureOpen();
        return stateMachine.list(namespace, group);
    }

    @Override
    public List<HistoryEntry> history(ConfigKey key, int limit) {
        ensureOpen();
        return stateMachine.history(key, limit);
    }

    @Override
    public List<String> namespaces() {
        ensureOpen();
        Set<String> result = new TreeSet<>(stateMachine.namespaces());
        // 内置分区标记条目（dataId 即命名空间名），与 AP 同一套派生逻辑
        for (ConfigEntry entry : stateMachine.list(ConfigConstants.SYSTEM_NAMESPACE,
                ConfigConstants.WORKSPACE_GROUP)) {
            result.add(entry.getKey().getDataId());
        }
        result.remove(ConfigConstants.SYSTEM_NAMESPACE);
        return new ArrayList<>(result);
    }

    @Override
    public List<String> groups(String namespace) {
        ensureOpen();
        String target = namespace == null || namespace.trim().isEmpty()
                ? ConfigConstants.DEFAULT_NAMESPACE : namespace.trim();
        Set<String> result = new TreeSet<>(stateMachine.groups(target));
        for (ConfigEntry entry : stateMachine.list(target, ConfigConstants.WORKSPACE_GROUP)) {
            result.add(entry.getKey().getDataId());
        }
        result.remove(ConfigConstants.WORKSPACE_GROUP);
        return new ArrayList<>(result);
    }

    @Override
    public void createNamespace(String namespace) {
        // workspace 元数据作为内置分区走同一套复制（cp-design.md §8，与 AP 一致）
        String target = Preconditions.checkNotBlank(namespace, "namespace");
        propose(new ConfigCommand(ConfigOperation.PUBLISH,
                new ConfigKey(ConfigConstants.SYSTEM_NAMESPACE,
                        ConfigConstants.WORKSPACE_GROUP, target),
                "", ConfigType.TEXT, "system", System.currentTimeMillis()));
    }

    @Override
    public void createGroup(String namespace, String group) {
        String targetGroup = Preconditions.checkNotBlank(group, "group");
        String targetNamespace = namespace == null || namespace.trim().isEmpty()
                ? ConfigConstants.DEFAULT_NAMESPACE : namespace.trim();
        propose(new ConfigCommand(ConfigOperation.PUBLISH,
                new ConfigKey(targetNamespace, ConfigConstants.WORKSPACE_GROUP, targetGroup),
                "", ConfigType.TEXT, "system", System.currentTimeMillis()));
    }

    @Override
    public NodeStatus status() {
        return new NodeStatus(nodeId, host, httpPort, remotingPort,
                ProtocolMode.CP, toNodeRole(raftNode.role()), !closed,
                raftNode.term(), raftNode.leaderId(), members.size());
    }

    @Override
    public ClusterStatus clusterStatus() {
        List<NodeStatus> nodes = new ArrayList<>(members.size());
        for (ClusterMember member : members) {
            if (member.getNodeId().equals(nodeId)) {
                nodes.add(status());
            } else {
                // CP-2：对端 role/term 无采集通道（CP-5 RAFT_STATUS 自报），
                // 健康位来自本节点与对端的真实投票/心跳交互
                nodes.add(new NodeStatus(member.getNodeId(), member.getHost(),
                        member.getHttpPort(), member.getRemotingPort(),
                        ProtocolMode.CP, NodeRole.FOLLOWER,
                        raftNode.isPeerHealthy(member.getNodeId()),
                        0L, "", members.size()));
            }
        }
        return new ClusterStatus(clusterId, nodeId, ProtocolMode.CP, nodes);
    }

    @Override
    public void addChangeListener(ChangeListener listener) {
        stateMachine.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        stateMachine.removeChangeListener(listener);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        raftNode.close();
        transport.close();
        try {
            remotingClient.shutdown();
        } catch (RuntimeException e) {
            log.warn("节点间调用客户端关闭异常", e);
        }
        raftLog.close();
    }

    /**
     * 返回 Raft 节点（供 RAFT_VOTE / RAFT_APPEND 处理器投递消息）。
     *
     * @return Raft 节点
     */
    public RaftNode raftNode() {
        return raftNode;
    }

    /**
     * 已提交条目 apply 到状态机（状态线程调用）：revision = raft index，
     * version = 命令携带的 assignedVersion（leader 在 prepare 阶段分配）。
     * 经 {@link #applyFromLog} 幂等落地（转发节点提前 apply 的同 index
     * 重放与更旧条目晚到都跳过）。
     */
    private void onApply(long index, long term, byte[] payload) {
        ConfigCommand command = CommandCodec.decode(payload);
        Long assigned = command.getAssignedVersion();
        applyFromLog(command, index, assigned != null ? assigned : index);
        log.debug("apply 已提交条目: index={}, term={}, op={}, key={}",
                index, term, command.getOperation(), command.getKey());
    }

    /**
     * 日志路径 apply（状态线程；与提前 apply 路径方法级互斥）。
     *
     * <p>幂等三闸：per-key 提前 apply 水位（version ≤ 水位跳过——含提前
     * apply 自身的重放与更旧条目晚到）；状态机已有同版或更新（跳过）；
     * 删除遇 key 已不存在（另一路径已 apply）。无论跳否都推进
     * {@link #logAppliedRevision}（连续位点，供提前 apply 路径判"是否已
     * 按序走过本条"）。</p>
     */
    private synchronized void applyFromLog(ConfigCommand command, long revision,
                                           long version) {
        ConfigKey key = command.getKey();
        Long watermark = earlyWatermark.get(key);
        if (watermark != null && version <= watermark) {
            log.debug("幂等跳过（已被提前 apply 或更旧）: key={}, version={}, watermark={}",
                    key.toPath(), version, watermark);
            logAppliedRevision = revision;
            return;
        }
        if (isSuperseded(command, version)) {
            log.debug("幂等跳过（状态机已有同版或更新）: key={}, version={}",
                    key.toPath(), version);
            logAppliedRevision = revision;
            return;
        }
        stateMachine.apply(command, revision, version);
        logAppliedRevision = revision;
    }

    /**
     * 转发节点提前 apply（RAFT_FORWARD 应答后、转发线程调用；与日志路径
     * 方法级互斥）：写后即读闭环。
     *
     * <p>{@code revision ≤ logAppliedRevision} 说明日志路径已按序 apply 过
     * 本条（复制先于应答到达的竞态），直接跳过；否则落地本条并登记 per-key
     * 水位，挡住随后送达的同条重放与更低 index 的旧条晚到。</p>
     */
    private synchronized void applyForwarded(ConfigCommand command, long revision,
                                             long version) {
        if (revision <= logAppliedRevision) {
            log.debug("幂等跳过（日志路径已按序 apply）: key={}, revision={}",
                    command.getKey().toPath(), revision);
            return;
        }
        ConfigKey key = command.getKey();
        Long watermark = earlyWatermark.get(key);
        if (watermark != null && version <= watermark) {
            log.debug("幂等跳过（已提前 apply）: key={}, version={}", key.toPath(), version);
            return;
        }
        if (isSuperseded(command, version)) {
            log.debug("幂等跳过（状态机已有同版或更新）: key={}, version={}",
                    key.toPath(), version);
            return;
        }
        earlyWatermark.merge(key, version, Math::max);
        stateMachine.apply(command, revision, version);
    }

    /**
     * 状态机是否已有同版或更新（跳过判定）；删除遇 key 已不存在视为已 apply。
     */
    private boolean isSuperseded(ConfigCommand command, long version) {
        Optional<ConfigEntry> existing = stateMachine.get(command.getKey());
        if (command.getOperation() == ConfigOperation.DELETE) {
            return !existing.isPresent();
        }
        return existing.map(entry -> entry.getVersion() >= version).orElse(false);
    }

    /**
     * 校验乐观锁期望版本（D5：底账 = 已 apply 状态机 + 本 leader 在途叠加
     * 水位；命令不携带 expectedVersion 落盘，回放不会重复校验——与
     * LocalEngine 同语义）。
     */
    private void checkExpectedVersion(ConfigCommand command) {
        Long expected = command.getExpectedVersion();
        if (expected == null) {
            return;
        }
        ConfigKey key = command.getKey();
        long current = stateMachine.get(key)
                .map(entry -> entry.getVersion())
                .orElse(-1L);
        // 叠加在途写（状态线程按序执行，水位与校验天然互斥）
        for (PendingWrite pending : pendingWrites.values()) {
            if (pending.key.equals(key) && pending.version > current) {
                current = pending.version;
            }
        }
        if (expected.longValue() != current) {
            throw new PolyConfigException(ErrorCode.VERSION_CONFLICT,
                    "配置版本冲突: key=" + key
                            + ", expected=" + expected + ", current=" + current);
        }
    }

    private static long awaitIndex(CompletableFuture<Long> applied, long timeoutMillis) {
        try {
            return applied.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "等待写提交被中断");
        } catch (TimeoutException e) {
            // 条目可能随后提交（at-least-once）：在途水位保留到 apply/step-down
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "写在 " + timeoutMillis + "ms 内未提交（多数派不可用或正在选举），请重试");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PolyConfigException) {
                throw (PolyConfigException) cause;
            }
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "写提交失败", cause);
        }
    }

    private static NodeRole toNodeRole(RaftRole role) {
        switch (role) {
            case LEADER:
                return NodeRole.LEADER;
            case CANDIDATE:
                return NodeRole.CANDIDATE;
            case FOLLOWER:
            default:
                return NodeRole.FOLLOWER;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "协议已关闭");
        }
    }

    /**
     * D5 在途写（已追加未提交）：key + leader 分配的 version（= raft index）。
     */
    private static final class PendingWrite {

        private final ConfigKey key;

        private final long version;

        private PendingWrite(ConfigKey key, long version) {
            this.key = key;
            this.version = version;
        }
    }
}
