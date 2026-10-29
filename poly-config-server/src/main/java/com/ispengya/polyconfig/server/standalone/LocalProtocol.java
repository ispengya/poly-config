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
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 集群 ID。
     */
    private final String clusterId;

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
    public NodeStatus status() {
        return new NodeStatus(nodeId, ProtocolMode.STANDALONE, NodeRole.STANDALONE,
                !closed, 0L, "", 1);
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
