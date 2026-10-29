package com.ispengya.polyconfig.server.standalone;

import com.ispengya.polyconfig.common.codec.CommandCodec;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.state.ConfigStateMachine;
import com.ispengya.polyconfig.core.workspace.WorkspaceRegistry;
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * 单机协议实现。
 *
 * <p>命令先追加并刷盘，再应用到状态机；启动时从日志回放恢复全部配置。</p>
 */
public class LocalProtocol implements ConfigProtocol {

    private static final Logger log = LoggerFactory.getLogger(LocalProtocol.class);

    /**
     * 配置日志。
     */
    private final SegmentLog segmentLog;

    /**
     * 配置状态机。
     */
    private final ConfigStateMachine stateMachine;

    /**
     * 命名空间与分组元数据。
     */
    private final WorkspaceRegistry workspaceRegistry;

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
     * 已应用的命令序号。
     */
    private long revision;

    /**
     * 是否已关闭。
     */
    private boolean closed;

    /**
     * 创建单机协议并回放日志。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     */
    public LocalProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.stateMachine = new ConfigStateMachine(historyLimit);
        this.workspaceRegistry = new WorkspaceRegistry(dataDir.resolve("workspaces.json"));
        this.segmentLog = new SegmentLog(SegmentLogConfig.builder(dataDir.resolve("log"))
                .syncOnAppend(false)
                .build());
        this.segmentLog.scan((offset, payload) -> {
            ConfigCommand command = CommandCodec.decode(payload);
            revision++;
            stateMachine.apply(command, revision);
            return true;
        });
        if (segmentLog.getRecoveryTruncatedOffset() >= 0) {
            log.warn("配置日志在启动时发生截断, truncatedOffset={}",
                    segmentLog.getRecoveryTruncatedOffset());
        }
    }

    /**
     * 更新节点对外地址。
     *
     * @param host 主机名或 IP
     * @param httpPort HTTP 端口
     * @param remotingPort 客户端长连接端口
     */
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
    public synchronized MutationResult propose(ConfigCommand command) {
        ensureOpen();
        byte[] payload = CommandCodec.encode(command);
        segmentLog.append(payload);
        segmentLog.flush();
        revision++;
        stateMachine.apply(command, revision);
        String md5 = command.getOperation() == ConfigOperation.DELETE
                ? "" : Md5Utils.md5Hex(command.getContent());
        return new MutationResult(command.getKey(), revision, revision, md5);
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        ensureOpen();
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
        Set<String> result = new TreeSet<>(workspaceRegistry.namespaces());
        result.addAll(stateMachine.namespaces());
        return new ArrayList<>(result);
    }

    @Override
    public List<String> groups(String namespace) {
        ensureOpen();
        Set<String> result = new TreeSet<>(workspaceRegistry.groups(namespace));
        result.addAll(stateMachine.groups(namespace));
        return new ArrayList<>(result);
    }

    @Override
    public void createNamespace(String namespace) {
        ensureOpen();
        workspaceRegistry.createNamespace(namespace);
    }

    @Override
    public void createGroup(String namespace, String group) {
        ensureOpen();
        workspaceRegistry.createGroup(namespace, group);
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
        stateMachine.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        stateMachine.removeChangeListener(listener);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        segmentLog.close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "协议已关闭");
        }
    }
}
