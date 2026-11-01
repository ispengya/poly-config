package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ForwardReadRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteResult;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
 * <p>异步复制（AP-3）与校验/反熵（AP-4）尚未接入：非转发节点暂时看不到
 * 其他节点产生的变更。</p>
 */
public class DistroProtocol implements ConfigProtocol {

    private static final Logger log = LoggerFactory.getLogger(DistroProtocol.class);

    /**
     * 成员探测周期；仅用于状态页展示。
     */
    private static final long PROBE_INTERVAL_MILLIS = 5_000L;

    /**
     * 单次探测连接超时。
     */
    private static final long PROBE_TIMEOUT_MILLIS = 1_000L;

    /**
     * 健康判定窗口：最近一次探测成功距今超过该值视为不健康（3 个探测周期）。
     */
    private static final long HEALTH_WINDOW_MILLIS = 15_000L;

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
     * 节点间调用客户端（写转发、线性读）。
     */
    private final NettyRemotingClient remotingClient;

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
     * peer 最近一次探测成功时间（nodeId → epoch millis），仅喂状态页。
     */
    private final ConcurrentHashMap<String, Long> peerLastSeen = new ConcurrentHashMap<>();

    /**
     * 成员探测线程（纯展示用途，AP-4 校验接入后由校验结果取代）。
     */
    private final ScheduledExecutorService probeExecutor;

    /**
     * 创建 Distro 协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照触发阈值（命令条数），0 表示关闭快照
     * @param members 静态成员列表，不可为空且必须包含本节点
     */
    public DistroProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit,
                          int snapshotThreshold, List<ClusterMember> members) {
        this.assignments = new DistroAssignments(members);
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        // 构造即校验：本节点必须在成员列表中，否则分区归属会指向别的节点
        this.assignments.member(nodeId);
        this.engine = new LocalEngine(dataDir, nodeId, clusterId, historyLimit, snapshotThreshold);
        this.remotingClient = new NettyRemotingClient();
        this.probeExecutor =
                Executors.newSingleThreadScheduledExecutor(daemonFactory("poly-config-probe"));
    }

    @Override
    public void start() {
        remotingClient.start();
        List<ClusterMember> members = assignments.members();
        log.info("DistroProtocol started, nodeId={}, clusterId={}, members={}, 本节点负责分区占比≈1/{}",
                nodeId, clusterId, members, members.size());
        if (members.size() > 1) {
            probeExecutor.scheduleWithFixedDelay(this::probePeers,
                    1_000L, PROBE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
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
        return engine.applyReplicated(toCommand(result));
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
        // AP-3 起 workspace 元数据作为内置分区复制（docs/ap-design.md §8）；
        // 当前阶段为各节点本地登记
        engine.createNamespace(namespace);
    }

    @Override
    public void createGroup(String namespace, String group) {
        engine.createGroup(namespace, group);
    }

    @Override
    public NodeStatus status() {
        return new NodeStatus(nodeId, host, httpPort, remotingPort,
                ProtocolMode.AP, NodeRole.PEER, !closed, 0L, "",
                assignments.members().size());
    }

    @Override
    public ClusterStatus clusterStatus() {
        List<NodeStatus> nodes = new ArrayList<>();
        for (ClusterMember member : assignments.members()) {
            if (member.getNodeId().equals(nodeId)) {
                nodes.add(status());
            } else {
                nodes.add(new NodeStatus(member.getNodeId(), member.getHost(),
                        member.getHttpPort(), member.getRemotingPort(),
                        ProtocolMode.AP, NodeRole.PEER, isPeerHealthy(member.getNodeId()),
                        0L, "", assignments.members().size()));
            }
        }
        return new ClusterStatus(clusterId, nodeId, ProtocolMode.AP, nodes);
    }

    /**
     * 探测全部 peer 的长连接端口，刷新状态页健康位。
     *
     * <p><b>纯展示用途</b>：探测结果不参与任何协议决策（写转发、复制、校验
     * 各自按自己的交互判断对端状态）。AP-4 校验接入后，健康位改由"最近一次
     * 校验/复制成功时间"点亮，本探测线程随之移除。</p>
     *
     * <p>整体捕获异常：{@code scheduleWithFixedDelay} 在任务抛出时会取消
     * 后续执行，探测失败绝不能中断调度。</p>
     */
    private void probePeers() {
        try {
            long now = System.currentTimeMillis();
            for (ClusterMember member : assignments.members()) {
                if (member.getNodeId().equals(nodeId)) {
                    continue;
                }
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(member.getHost(),
                            member.getRemotingPort()), (int) PROBE_TIMEOUT_MILLIS);
                    peerLastSeen.put(member.getNodeId(), now);
                } catch (Exception ignored) {
                    // 不可达：保持旧时间戳，超过健康窗口后自然转为不健康
                }
            }
        } catch (RuntimeException e) {
            log.debug("成员探测轮次异常", e);
        }
    }

    /**
     * 判断 peer 健康位（仅展示）：窗口内探测成功过即健康。
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
        probeExecutor.shutdownNow();
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

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger index = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
