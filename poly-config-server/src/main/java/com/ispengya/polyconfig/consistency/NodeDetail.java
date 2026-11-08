package com.ispengya.polyconfig.consistency;

import java.util.ArrayList;
import java.util.List;

/**
 * 节点详细状态（AP 状态采集，{@code DISTRO_STATUS} 应答体）。
 *
 * <p>与 {@link NodeStatus} 的差别：NodeStatus 是"别人看我"的静态视图，
 * NodeDetail 是"我自己上报"的完整视图——拥有的分区、校验/健康时间戳、
 * 校验异常统计。控制台所在节点向全体成员收集后按 nodeId 聚合展示；
 * 对端不可达时降级为静态信息。</p>
 */
public class NodeDetail {

    /**
     * 节点 ID。
     */
    private String nodeId;

    /**
     * 对外主机名或 IP。
     */
    private String host;

    /**
     * HTTP 端口。
     */
    private int port;

    /**
     * 客户端长连接端口。
     */
    private int remotingPort;

    /**
     * 协议模式。
     */
    private ProtocolMode mode;

    /**
     * 节点角色。
     */
    private NodeRole role;

    /**
     * 节点是否健康。
     */
    private boolean healthy;

    /**
     * 集群成员数量。
     */
    private int memberCount;

    /**
     * 本节点本地日志位点（节点本地序号，仅展示，不可跨节点比较）。
     */
    private long currentRevision;

    // ===== CP（RAFT_STATUS 自报）字段：AP 详情保持默认值 =====

    /**
     * CP：本节点已知的 leader（"" 表示选举中/未知）。
     */
    private String leaderId = "";

    /**
     * CP：本节点当前任期。
     */
    private long term;

    /**
     * CP：本节点 commitIndex。
     */
    private long commitIndex;

    /**
     * CP：本节点 lastApplied。
     */
    private long lastApplied;

    /**
     * CP：本节点快照位点（lastIncludedIndex，0 = 无快照）。
     */
    private long snapshotIndex;

    /**
     * 本节点拥有的分区列表（{@code namespace/group}，有序去重）。
     */
    private List<String> ownedPartitions = new ArrayList<>();

    /**
     * 累计校验异常次数（副本多余键 / 版本反超等，见设计 §6）。
     */
    private long anomalyCount;

    /**
     * 最近一次校验异常时间（epoch millis，0 表示从未）。
     */
    private long lastAnomalyTime;

    /**
     * 最近一次校验异常描述。
     */
    private String lastAnomaly = "";

    /**
     * 本节点对各 peer 的交互视图（健康/校验时间/上次校验增量）。
     */
    private List<PeerView> peers = new ArrayList<>();

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getRemotingPort() {
        return remotingPort;
    }

    public void setRemotingPort(int remotingPort) {
        this.remotingPort = remotingPort;
    }

    public ProtocolMode getMode() {
        return mode;
    }

    public void setMode(ProtocolMode mode) {
        this.mode = mode;
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    public int getMemberCount() {
        return memberCount;
    }

    public void setMemberCount(int memberCount) {
        this.memberCount = memberCount;
    }

    public long getCurrentRevision() {
        return currentRevision;
    }

    public void setCurrentRevision(long currentRevision) {
        this.currentRevision = currentRevision;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId == null ? "" : leaderId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    public void setSnapshotIndex(long snapshotIndex) {
        this.snapshotIndex = snapshotIndex;
    }

    public List<String> getOwnedPartitions() {
        return ownedPartitions;
    }

    public void setOwnedPartitions(List<String> ownedPartitions) {
        this.ownedPartitions = ownedPartitions == null ? new ArrayList<String>() : ownedPartitions;
    }

    public long getAnomalyCount() {
        return anomalyCount;
    }

    public void setAnomalyCount(long anomalyCount) {
        this.anomalyCount = anomalyCount;
    }

    public long getLastAnomalyTime() {
        return lastAnomalyTime;
    }

    public void setLastAnomalyTime(long lastAnomalyTime) {
        this.lastAnomalyTime = lastAnomalyTime;
    }

    public String getLastAnomaly() {
        return lastAnomaly;
    }

    public void setLastAnomaly(String lastAnomaly) {
        this.lastAnomaly = lastAnomaly == null ? "" : lastAnomaly;
    }

    public List<PeerView> getPeers() {
        return peers;
    }

    public void setPeers(List<PeerView> peers) {
        this.peers = peers == null ? new ArrayList<PeerView>() : peers;
    }

    /**
     * 本节点对单个 peer 的交互视图。
     */
    public static class PeerView {

        /**
         * peer 节点 ID。
         */
        private String nodeId;

        /**
         * 最近一次复制/校验交互成功时间（epoch millis，0 表示从未）——健康数据源。
         */
        private long lastSeen;

        /**
         * 最近一次校验成功时间（epoch millis，0 表示从未）。
         */
        private long lastVerify;

        /**
         * 最近一次校验实际补齐的条数（0 表示已对齐）。
         */
        private long lastDeltaCount;

        /**
         * CP：leader 视角该 follower 已确认复制到的 index（0 = 未知/非 leader）。
         */
        private long matchIndex;

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public long getMatchIndex() {
            return matchIndex;
        }

        public void setMatchIndex(long matchIndex) {
            this.matchIndex = matchIndex;
        }

        public long getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(long lastSeen) {
            this.lastSeen = lastSeen;
        }

        public long getLastVerify() {
            return lastVerify;
        }

        public void setLastVerify(long lastVerify) {
            this.lastVerify = lastVerify;
        }

        public long getLastDeltaCount() {
            return lastDeltaCount;
        }

        public void setLastDeltaCount(long lastDeltaCount) {
            this.lastDeltaCount = lastDeltaCount;
        }
    }
}
