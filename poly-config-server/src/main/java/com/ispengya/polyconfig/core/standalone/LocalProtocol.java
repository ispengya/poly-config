package com.ispengya.polyconfig.core.standalone;

import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.engine.LocalEngine;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 单机协议实现。
 *
 * <p>存储与单机链路委托 {@link LocalEngine}，本类只负责 ConfigProtocol 语义
 * （状态上报、地址维护）。命令的组提交、快照与恢复细节见引擎。</p>
 */
public class LocalProtocol implements ConfigProtocol {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(LocalProtocol.class);

    /**
     * 本地存储引擎。
     */
    private final LocalEngine engine;

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
     * 创建单机协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     */
    public LocalProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit) {
        this(dataDir, nodeId, clusterId, historyLimit, 0);
    }

    /**
     * 创建单机协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照触发阈值（命令条数），0 表示关闭快照
     */
    public LocalProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit,
                         int snapshotThreshold) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.engine = new LocalEngine(dataDir, nodeId, clusterId, historyLimit, snapshotThreshold);
    }

    /**
     * 更新节点对外地址。
     *
     * @param host 主机名或 IP
     * @param httpPort HTTP 端口
     * @param remotingPort 客户端长连接端口
     */
    @Override
    public void setNodeAddress(String host, int httpPort, int remotingPort) {
        this.host = host == null ? "" : host;
        this.httpPort = httpPort;
        this.remotingPort = remotingPort;
    }

    @Override
    public void start() {
        log.info("LocalProtocol started, nodeId={}, clusterId={}", nodeId, clusterId);
    }

    @Override
    public MutationResult propose(ConfigCommand command) {
        return engine.proposeLocal(command);
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        return engine.read(key);
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
        engine.createNamespace(namespace);
    }

    @Override
    public void createGroup(String namespace, String group) {
        engine.createGroup(namespace, group);
    }

    @Override
    public NodeStatus status() {
        return new NodeStatus(nodeId, host, httpPort, remotingPort,
                ProtocolMode.STANDALONE, NodeRole.STANDALONE, !closed, 0L, "", 1);
    }

    @Override
    public ClusterStatus clusterStatus() {
        return new ClusterStatus(clusterId, nodeId, ProtocolMode.STANDALONE,
                Collections.singletonList(status()));
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
        engine.close();
    }

    /**
     * 返回本地存储引擎。
     *
     * @return 存储引擎
     */
    public LocalEngine engine() {
        return engine;
    }
}
