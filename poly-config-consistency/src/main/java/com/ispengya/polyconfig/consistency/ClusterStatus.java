package com.ispengya.polyconfig.consistency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 当前节点视角的集群状态。
 *
 * <p>控制台通过任意节点的管理 API 获取该视图，不需要直接连接每个 server。</p>
 */
public final class ClusterStatus {

    /**
     * 集群 ID。
     */
    private final String clusterId;

    /**
     * 当前节点 ID。
     */
    private final String currentNodeId;

    /**
     * 集群运行模式。
     */
    private final ProtocolMode mode;

    /**
     * 当前节点可见的成员状态。
     */
    private final List<NodeStatus> nodes;

    /**
     * 创建集群状态。
     *
     * @param clusterId 集群 ID
     * @param currentNodeId 当前节点 ID
     * @param mode 集群运行模式
     * @param nodes 成员状态列表
     */
    public ClusterStatus(String clusterId, String currentNodeId, ProtocolMode mode,
                         List<NodeStatus> nodes) {
        this.clusterId = clusterId == null ? "" : clusterId;
        this.currentNodeId = currentNodeId == null ? "" : currentNodeId;
        this.mode = mode;
        this.nodes = nodes == null
                ? Collections.<NodeStatus>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(nodes));
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getCurrentNodeId() {
        return currentNodeId;
    }

    public ProtocolMode getMode() {
        return mode;
    }

    public List<NodeStatus> getNodes() {
        return nodes;
    }
}
