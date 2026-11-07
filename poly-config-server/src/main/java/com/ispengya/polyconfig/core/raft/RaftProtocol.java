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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * CP（Raft）协议实现（cp-design.md）。
 *
 * <p>形态：单 leader 写、多数派提交、日志复制；读默认 LOCAL（任意节点读
 * 本地状态机），LINEARIZABLE 走 leader + read-index（CP-4）。写应答 =
 * commit + apply 之后（D7）；转发节点先 apply 再应答的机制在 CP-3 随
 * RAFT_FORWARD 接入。</p>
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
        // D5：校验与赋版本在状态线程内按序执行（CP-2 单成员下即"已 apply 视图"）
        CompletableFuture<Long> applied = raftNode.propose(nextIndex -> {
            checkExpectedVersion(command);
            command.setAssignedVersion(nextIndex);
            return CommandCodec.encode(command);
        });
        long index = awaitIndex(applied);
        long version = command.getAssignedVersion() != null
                ? command.getAssignedVersion() : index;
        String md5 = command.getOperation() == ConfigOperation.DELETE
                ? "" : Md5Utils.md5Hex(command.getContent());
        return new MutationResult(command.getKey(), version, index, md5);
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
     */
    private void onApply(long index, long term, byte[] payload) {
        ConfigCommand command = CommandCodec.decode(payload);
        Long assigned = command.getAssignedVersion();
        stateMachine.apply(command, index, assigned != null ? assigned : index);
        log.debug("apply 已提交条目: index={}, term={}, op={}, key={}",
                index, term, command.getOperation(), command.getKey());
    }

    /**
     * 校验乐观锁期望版本（与已 apply 的 version 比较；命令不携带
     * expectedVersion 落盘，回放不会重复校验——与 LocalEngine 同语义）。
     */
    private void checkExpectedVersion(ConfigCommand command) {
        Long expected = command.getExpectedVersion();
        if (expected == null) {
            return;
        }
        long current = stateMachine.get(command.getKey())
                .map(entry -> entry.getVersion())
                .orElse(-1L);
        if (expected.longValue() != current) {
            throw new PolyConfigException(ErrorCode.VERSION_CONFLICT,
                    "配置版本冲突: key=" + command.getKey()
                            + ", expected=" + expected + ", current=" + current);
        }
    }

    private static long awaitIndex(CompletableFuture<Long> applied) {
        try {
            return applied.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "等待写提交被中断");
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
}
