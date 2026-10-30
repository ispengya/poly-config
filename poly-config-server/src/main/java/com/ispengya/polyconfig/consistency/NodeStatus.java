package com.ispengya.polyconfig.consistency;

/**
 * 节点运行状态。
 */
public final class NodeStatus {

    /**
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 节点对外主机名或 IP。
     */
    private final String host;

    /**
     * HTTP 端口。
     */
    private final int port;

    /**
     * 客户端长连接端口。
     */
    private final int remotingPort;

    /**
     * 协议模式。
     */
    private final ProtocolMode mode;

    /**
     * 节点角色。
     */
    private final NodeRole role;

    /**
     * 节点是否健康。
     */
    private final boolean healthy;

    /**
     * 当前任期；单机模式和 AP 模式为 0。
     */
    private final long term;

    /**
     * 当前 leader 节点 ID；单机模式和 AP 模式为空字符串。
     */
    private final String leaderId;

    /**
     * 集群成员数量。
     */
    private final int memberCount;

    /**
     * 创建节点状态。
     *
     * @param nodeId 节点 ID
     * @param host 节点对外主机名或 IP
     * @param port HTTP 端口
     * @param remotingPort 客户端长连接端口
     * @param mode 协议模式
     * @param role 节点角色
     * @param healthy 是否健康
     * @param term 当前任期
     * @param leaderId leader 节点 ID
     * @param memberCount 集群成员数量
     */
    public NodeStatus(String nodeId, String host, int port, int remotingPort,
                      ProtocolMode mode, NodeRole role, boolean healthy,
                      long term, String leaderId, int memberCount) {
        this.nodeId = nodeId;
        this.host = host == null ? "" : host;
        this.port = port;
        this.remotingPort = remotingPort;
        this.mode = mode;
        this.role = role;
        this.healthy = healthy;
        this.term = term;
        this.leaderId = leaderId == null ? "" : leaderId;
        this.memberCount = memberCount;
    }

    /**
     * 兼容旧调用，地址字段为空。
     *
     * @param nodeId 节点 ID
     * @param mode 协议模式
     * @param role 节点角色
     * @param healthy 是否健康
     * @param term 当前任期
     * @param leaderId leader 节点 ID
     * @param memberCount 集群成员数量
     */
    public NodeStatus(String nodeId, ProtocolMode mode, NodeRole role, boolean healthy,
                      long term, String leaderId, int memberCount) {
        this(nodeId, "", 0, 0, mode, role, healthy, term, leaderId, memberCount);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getRemotingPort() {
        return remotingPort;
    }

    public ProtocolMode getMode() {
        return mode;
    }

    public NodeRole getRole() {
        return role;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getMemberCount() {
        return memberCount;
    }
}
