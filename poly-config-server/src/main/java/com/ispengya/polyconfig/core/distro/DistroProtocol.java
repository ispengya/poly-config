package com.ispengya.polyconfig.core.distro;

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
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ForwardReadRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteResult;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.VerifyEntry;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.common.util.Preconditions;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeDetail;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AP（Distro）协议实现。
 *
 * <p>形态见 docs/ap-design.md：全量副本 + 分片写责任，分区粒度为
 * namespace + group，owner 由静态成员列表推导；读和推送本地化。</p>
 *
 * <p>写路径：owner 分区本机 {@code proposeLocal}；非 owner 分区经
 * {@code DISTRO_FORWARD} 转发给 owner，收到应答后在转发节点同步 apply
 * （写后即读）并落本地日志。{@code LINEARIZABLE} 读转发 owner；
 * 默认 {@code LOCAL} 读本地副本。</p>
 *
 * <p>异步复制：owner 本地写刷盘后入 {@link DistroReplicator} 攒批，
 * {@code DISTRO_SYNC} 推送其余副本；副本幂等 apply 并触发本地推送。</p>
 *
 * <p>周期校验/反熵：{@link DistroVerifier} 每轮向全体成员送分区摘要
 * （含墓碑），应答增量按幂等规则应用——补齐复制失败/宕机期间的缺口，
 * 同时充当探活（健康位 = 最近一次复制/校验成功，纯展示 TCP 探测已移除）。</p>
 */
public class DistroProtocol implements ConfigProtocol {

    private static final Logger log = LoggerFactory.getLogger(DistroProtocol.class);

    /**
     * 健康判定窗口：最近一次复制/校验成功距今超过该值视为不健康（3 个周期）。
     */
    private static final long HEALTH_WINDOW_MILLIS = 15_000L;

    /**
     * 状态采集单次调用超时（控制台刷新不被慢对端拖死）。
     */
    private static final long STATUS_TIMEOUT_MILLIS = 500L;

    /**
     * 转发请求超时。
     */
    private static final long FORWARD_TIMEOUT_MILLIS = 3_000L;

    /**
     * 本地存储引擎（日志 + 状态机 + 组提交 + 快照）。
     */
    private final LocalEngine engine;

    /**
     * 分区归属表。
     */
    private final DistroAssignments assignments;

    /**
     * 节点间调用客户端（写转发、线性读、复制）。
     */
    private final NettyRemotingClient remotingClient;

    /**
     * owner 侧异步复制器（本地写刷盘后攒批推送其余副本）。
     */
    private final DistroReplicator replicator;

    /**
     * 周期校验器（分区摘要比对 + 增量补齐 + 探活）。
     */
    private final DistroVerifier verifier;

    /**
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 集群 ID。
     */
    private final String clusterId;

    /**
     * 节点对外主机名或 IP。
     */
    private volatile String host = "";

    /**
     * HTTP 端口。
     */
    private volatile int httpPort;

    /**
     * 客户端长连接端口。
     */
    private volatile int remotingPort;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * peer 最近一次复制/校验交互成功时间（nodeId → epoch millis），喂状态页健康位。
     */
    private final ConcurrentHashMap<String, Long> peerLastSeen = new ConcurrentHashMap<>();

    /**
     * 校验异常统计（校验处理器写入，状态采集读出）。
     */
    private final VerifyAnomalyStats anomalyStats = new VerifyAnomalyStats();

    /**
     * 创建 Distro 协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照触发阈值（命令条数），0 表示关闭快照
     * @param members 静态成员列表，不可为空且必须包含本节点
     * @param verifyIntervalMillis 校验周期（毫秒），决定收敛上界与探活频率
     */
    public DistroProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit,
                          int snapshotThreshold, List<ClusterMember> members,
                          long verifyIntervalMillis) {
        this.assignments = new DistroAssignments(members);
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        // 构造即校验：本节点必须在成员列表中，否则分区归属会指向别的节点
        this.assignments.member(nodeId);
        this.engine = new LocalEngine(dataDir, nodeId, clusterId, historyLimit, snapshotThreshold);
        this.remotingClient = new NettyRemotingClient();
        this.replicator = new DistroReplicator(assignments, nodeId, remotingClient,
                peerNodeId -> peerLastSeen.put(peerNodeId, System.currentTimeMillis()));
        this.verifier = new DistroVerifier(assignments, nodeId, engine, remotingClient,
                verifyIntervalMillis,
                peerNodeId -> peerLastSeen.put(peerNodeId, System.currentTimeMillis()));
        // 只有本地分配路径（owner）触发复制；副本 applyReplicated 不再向外复制
        this.engine.setProposeListener(replicator::enqueue);
    }

    @Override
    public void start() {
        remotingClient.start();
        replicator.start();
        verifier.start();
        List<ClusterMember> members = assignments.members();
        log.info("DistroProtocol started, nodeId={}, clusterId={}, members={}, 本节点负责分区占比≈1/{}",
                nodeId, clusterId, members, members.size());
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
        ClusterMember owner = assignments.ownerOf(command.getKey());
        if (owner.getNodeId().equals(nodeId) || assignments.members().size() == 1) {
            return engine.proposeLocal(command);
        }
        return forwardWrite(owner, command);
    }

    /**
     * 把写请求转发给 owner，并在本节点同步 apply（写后即读）。
     *
     * <p>owner 应答携带分配的 version 与命令回显；本节点按该 version 落本地
     * 日志与状态机，随后才应答调用方——从本节点读必然可见。owner 不可达时
     * 报 {@code UNAVAILABLE}（owner 故障转移不在 v1 范围，见设计文档 §1）。</p>
     */
    private MutationResult forwardWrite(ClusterMember owner, ConfigCommand command) {
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_FORWARD, null);
        request.setBody(JsonUtils.toJson(ForwardWriteRequest.from(command))
                .getBytes(StandardCharsets.UTF_8));
        RemotingCommand response;
        try {
            response = remotingClient.invokeSync(address(owner), request, FORWARD_TIMEOUT_MILLIS);
        } catch (Exception e) {
            log.warn("写转发失败, owner={}, key={}: {}",
                    owner.getNodeId(), command.getKey().toPath(), e.toString());
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "分区 owner 不可用: owner=" + owner.getNodeId()
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
                    "写转发被 owner 拒绝: code=" + response.getCode() + ", " + describeResponse(response));
        }
        ForwardWriteResult result = JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8), ForwardWriteResult.class);
        if (result == null || result.getDataId() == null) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "写转发应答体非法");
        }
        // 按 owner 分配的版本同步 apply：写后即读 + 本地持久化
        MutationResult applied = engine.applyReplicated(toCommand(result));
        log.debug("写转发完成, owner={}, key={}, version={}",
                owner.getNodeId(), command.getKey().toPath(), applied.getVersion());
        return applied;
    }

    /**
     * 从转发应答还原携带 owner 分配版本的命令。
     */
    private static ConfigCommand toCommand(ForwardWriteResult result) {
        ConfigCommand command = new ConfigCommand(
                com.ispengya.polyconfig.common.model.ConfigOperation.valueOf(result.getOperation()),
                new ConfigKey(result.getNamespace(), result.getGroup(), result.getDataId()),
                result.getContent(),
                com.ispengya.polyconfig.common.model.ConfigType.from(result.getType()),
                result.getOperator(), result.getTimestamp());
        command.setAssignedVersion(result.getVersion());
        return command;
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        ensureOpen();
        if (consistency != ReadConsistency.LINEARIZABLE) {
            return engine.read(key);
        }
        ClusterMember owner = assignments.ownerOf(key);
        if (owner.getNodeId().equals(nodeId) || assignments.members().size() == 1) {
            return engine.read(key);
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_READ, null);
        request.setBody(JsonUtils.toJson(
                ForwardReadRequest.of(key.getNamespace(), key.getGroup(), key.getDataId()))
                .getBytes(StandardCharsets.UTF_8));
        RemotingCommand response;
        try {
            response = remotingClient.invokeSync(address(owner), request, FORWARD_TIMEOUT_MILLIS);
        } catch (Exception e) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "分区 owner 不可用: owner=" + owner.getNodeId() + ", key=" + key, e);
        }
        if (response.getCode() == ProtocolCodes.RESPONSE_NOT_FOUND) {
            return Optional.empty();
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "线性读被 owner 拒绝: code=" + response.getCode() + ", " + describeResponse(response));
        }
        ConfigView view = JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8), ConfigView.class);
        if (view == null || view.getDataId() == null) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "线性读应答体非法");
        }
        return Optional.of(new ConfigEntry(
                new ConfigKey(view.getNamespace(), view.getGroup(), view.getDataId()),
                view.getContent(),
                com.ispengya.polyconfig.common.model.ConfigType.from(view.getType()),
                view.getMd5(), view.getVersion(), view.getRevision(),
                view.getCreatedTime(), view.getModifiedTime(), view.getOperator()));
    }

    @Override
    public List<ConfigEntry> list(String namespace, String group) {
        return engine.list(namespace, group);
    }

    @Override
    public List<HistoryEntry> history(ConfigKey key, int limit) {
        return engine.history(key, limit);
    }

    @Override
    public List<String> namespaces() {
        return engine.namespaces();
    }

    @Override
    public List<String> groups(String namespace) {
        return engine.groups(namespace);
    }

    @Override
    public void createNamespace(String namespace) {
        // workspace 元数据作为内置分区走同一套复制（docs/ap-design.md §8）：
        // (_system/_workspaces, dataId=命名空间名) 标记条目，经 owner 路由与异步复制
        String target = Preconditions.checkNotBlank(namespace, "namespace");
        propose(new ConfigCommand(ConfigOperation.PUBLISH,
                new ConfigKey(ConfigConstants.SYSTEM_NAMESPACE,
                        ConfigConstants.WORKSPACE_GROUP, target),
                "", ConfigType.TEXT, "system", System.currentTimeMillis()));
    }

    @Override
    public void createGroup(String namespace, String group) {
        // 标记条目放在 (命名空间/_workspaces, dataId=分组名)：命名空间随之可见
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
                ProtocolMode.AP, NodeRole.PEER, !closed, 0L, "",
                assignments.members().size());
    }

    @Override
    public ClusterStatus clusterStatus() {
        List<ClusterMember> members = assignments.members();
        List<NodeStatus> nodes = new ArrayList<>();
        for (ClusterMember member : members) {
            if (member.getNodeId().equals(nodeId)) {
                nodes.add(status());
            } else {
                nodes.add(new NodeStatus(member.getNodeId(), member.getHost(),
                        member.getHttpPort(), member.getRemotingPort(),
                        ProtocolMode.AP, NodeRole.PEER, isPeerHealthy(member.getNodeId()),
                        0L, "", members.size()));
            }
        }
        ClusterStatus result = new ClusterStatus(clusterId, nodeId, ProtocolMode.AP, nodes);
        // 内部通道第四种用途（设计 §4）：向全体成员采集详细状态后聚合；
        // 对端不可达降级为静态信息，不阻塞控制台
        result.setDetails(collectDetails(members));
        return result;
    }

    /**
     * 向全体成员采集详细状态（按成员顺序）；本节点本地构建，对端走
     * {@code DISTRO_STATUS}，失败降级为静态详情。
     *
     * @param members 成员列表
     * @return 详细状态列表
     */
    private List<NodeDetail> collectDetails(List<ClusterMember> members) {
        List<NodeDetail> details = new ArrayList<>(members.size());
        for (ClusterMember member : members) {
            if (member.getNodeId().equals(nodeId)) {
                try {
                    details.add(localDetail());
                } catch (RuntimeException e) {
                    log.debug("本地详细状态构建失败", e);
                    details.add(staticDetail(member));
                }
                continue;
            }
            try {
                RemotingCommand request = RemotingCommand.createRequestCommand(
                        ProtocolCodes.DISTRO_STATUS, null);
                RemotingCommand response = remotingClient.invokeSync(
                        address(member), request, STATUS_TIMEOUT_MILLIS);
                if (response.getCode() == ProtocolCodes.RESPONSE_OK
                        && response.getBody() != null && response.getBody().length > 0) {
                    NodeDetail detail = JsonUtils.parseObject(
                            new String(response.getBody(), StandardCharsets.UTF_8),
                            NodeDetail.class);
                    if (detail != null && detail.getNodeId() != null) {
                        details.add(detail);
                        continue;
                    }
                }
                details.add(staticDetail(member));
            } catch (Exception e) {
                log.debug("状态采集失败, peer={}: {}", member.getNodeId(), e.toString());
                details.add(staticDetail(member));
            }
        }
        return details;
    }

    /**
     * 本节点的详细状态（自报视图：分区归属、校验/健康时间、异常统计）。
     *
     * @return 详细状态
     */
    public NodeDetail localDetail() {
        NodeDetail detail = new NodeDetail();
        detail.setNodeId(nodeId);
        detail.setHost(host);
        detail.setPort(httpPort);
        detail.setRemotingPort(remotingPort);
        detail.setMode(ProtocolMode.AP);
        detail.setRole(NodeRole.PEER);
        detail.setHealthy(!closed);
        detail.setMemberCount(assignments.members().size());
        detail.setCurrentRevision(engine.currentRevision());
        detail.setOwnedPartitions(ownedPartitions());
        detail.setAnomalyCount(anomalyStats.getCount());
        detail.setLastAnomalyTime(anomalyStats.getLastTime());
        detail.setLastAnomaly(anomalyStats.getLastDetail());
        List<NodeDetail.PeerView> views = new ArrayList<>();
        for (ClusterMember member : assignments.members()) {
            if (member.getNodeId().equals(nodeId)) {
                continue;
            }
            NodeDetail.PeerView view = new NodeDetail.PeerView();
            view.setNodeId(member.getNodeId());
            Long seen = peerLastSeen.get(member.getNodeId());
            view.setLastSeen(seen == null ? 0L : seen);
            DistroVerifier.VerifyStat stat = verifier.statOf(member.getNodeId());
            if (stat != null) {
                view.setLastVerify(stat.getLastVerify());
                view.setLastDeltaCount(stat.getLastDelta());
            }
            views.add(view);
        }
        detail.setPeers(views);
        return detail;
    }

    /**
     * 本节点拥有的分区（现有分区按归属过滤，{@code namespace/group} 有序去重）。
     *
     * @return 分区列表
     */
    private List<String> ownedPartitions() {
        Set<String> result = new TreeSet<>();
        for (VerifyEntry entry : engine.verifyTable()) {
            ConfigKey key = new ConfigKey(entry.getNamespace(), entry.getGroup(),
                    entry.getDataId());
            if (assignments.ownerOf(key).getNodeId().equals(nodeId)) {
                result.add(key.getNamespace() + "/" + key.getGroup());
            }
        }
        return new ArrayList<>(result);
    }

    /**
     * 采集失败时的静态降级详情（只有身份信息与健康位，统计留空）。
     *
     * @param member 成员
     * @return 静态详情
     */
    private NodeDetail staticDetail(ClusterMember member) {
        NodeDetail detail = new NodeDetail();
        detail.setNodeId(member.getNodeId());
        detail.setHost(member.getHost());
        detail.setPort(member.getHttpPort());
        detail.setRemotingPort(member.getRemotingPort());
        detail.setMode(ProtocolMode.AP);
        detail.setRole(NodeRole.PEER);
        detail.setHealthy(isPeerHealthy(member.getNodeId()));
        detail.setMemberCount(assignments.members().size());
        return detail;
    }

    /**
     * 校验异常统计（状态采集读出）。
     *
     * @return 异常统计
     */
    public VerifyAnomalyStats anomalyStats() {
        return anomalyStats;
    }

    /**
     * 判断 peer 健康位（仅展示）：窗口内有过成功的复制/校验交互即健康。
     *
     * @param peerNodeId peer 节点 ID
     * @return 是否健康
     */
    private boolean isPeerHealthy(String peerNodeId) {
        Long lastSeen = peerLastSeen.get(peerNodeId);
        return lastSeen != null
                && System.currentTimeMillis() - lastSeen <= HEALTH_WINDOW_MILLIS;
    }

    @Override
    public void addChangeListener(ChangeListener listener) {
        engine.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        engine.removeChangeListener(listener);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        verifier.close();
        replicator.close();
        try {
            remotingClient.shutdown();
        } catch (RuntimeException e) {
            log.warn("节点间调用客户端关闭异常", e);
        }
        engine.close();
    }

    /**
     * 返回本地存储引擎（供 owner 侧转发处理器使用）。
     *
     * @return 存储引擎
     */
    public LocalEngine engine() {
        return engine;
    }

    /**
     * 返回分区归属表。
     *
     * @return 分区归属表
     */
    public DistroAssignments assignments() {
        return assignments;
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "协议已关闭");
        }
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
}
