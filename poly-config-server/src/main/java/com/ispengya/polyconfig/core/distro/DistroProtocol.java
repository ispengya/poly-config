package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.standalone.LocalProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
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
 * namespace + group，owner 由静态成员列表推导；读和推送全部本地化，
 * 写在 AP-2 里程碑接入转发 owner。</p>
 *
 * <p>AP-1 骨架：存储与单机链路完全委托 {@link LocalProtocol}（本地引擎），
 * 本节点行为与 standalone 等价；对外报告 AP 模式与静态成员视图。
 * 写转发（AP-2）、异步复制（AP-3）、校验/反熵（AP-4）尚未接入。</p>
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
     * 本地存储引擎（日志 + 状态机 + 组提交 + 快照）。
     */
    private final LocalProtocol local;

    /**
     * 分区归属表。
     */
    private final DistroAssignments assignments;

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
        this.local = new LocalProtocol(dataDir, nodeId, clusterId, historyLimit, snapshotThreshold);
        this.probeExecutor =
                Executors.newSingleThreadScheduledExecutor(daemonFactory("poly-config-probe"));
    }

    @Override
    public void start() {
        local.start();
        List<ClusterMember> members = assignments.members();
        log.info("DistroProtocol started, nodeId={}, clusterId={}, members={}, 本节点负责分区占比≈1/{}",
                nodeId, clusterId, members, members.size());
        if (members.size() > 1) {
            probeExecutor.scheduleWithFixedDelay(this::probePeers,
                    1_000L, PROBE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
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

    @Override
    public void setNodeAddress(String host, int httpPort, int remotingPort) {
        this.host = host == null ? "" : host;
        this.httpPort = httpPort;
        this.remotingPort = remotingPort;
        local.setNodeAddress(host, httpPort, remotingPort);
    }

    @Override
    public MutationResult propose(ConfigCommand command) {
        // AP-2 接入写转发：非 owner 分区转发到 owner；
        // AP-1 骨架阶段所有节点独立接受本地写（行为等价 standalone）
        return local.propose(command);
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        // 全量副本：LOCAL 读本地；LINEARIZABLE 需转发 owner 读（AP-2 之后生效）
        return local.read(key, consistency);
    }

    @Override
    public List<ConfigEntry> list(String namespace, String group) {
        return local.list(namespace, group);
    }

    @Override
    public List<HistoryEntry> history(ConfigKey key, int limit) {
        return local.history(key, limit);
    }

    @Override
    public List<String> namespaces() {
        return local.namespaces();
    }

    @Override
    public List<String> groups(String namespace) {
        return local.groups(namespace);
    }

    @Override
    public void createNamespace(String namespace) {
        // AP-2 起 workspace 元数据作为内置分区复制（docs/ap-design.md §8）
        local.createNamespace(namespace);
    }

    @Override
    public void createGroup(String namespace, String group) {
        local.createGroup(namespace, group);
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

    @Override
    public void addChangeListener(ChangeListener listener) {
        local.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        local.removeChangeListener(listener);
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
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        probeExecutor.shutdownNow();
        local.close();
    }

    /**
     * 返回分区归属表。
     *
     * @return 分区归属表
     */
    public DistroAssignments assignments() {
        return assignments;
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
