package com.ispengya.polyconfig.common.protocol;

/**
 * CP InstallSnapshot 请求（RAFT_SNAPSHOT，4003：leader → 落后 follower）。
 *
 * <p>整份快照下发：{@code snapshot} 为快照 JSON 文本（快照数据结构属于
 * 服务端存储层，不随协议层引入，故以序列化文本承载）；lastIncludedIndex/
 * lastIncludedTerm 与快照内元数据必须一致——快照视作该位置的一条伪日志
 * 条目（cp-design.md §7）。</p>
 */
public class SnapshotRequest {

    /**
     * 发送方（leader）当前任期。
     */
    private long term;

    /**
     * leader 节点 ID。
     */
    private String leaderId;

    /**
     * 快照覆盖到的最后一条日志 index（含）。
     */
    private long lastIncludedIndex;

    /**
     * 快照覆盖到的最后一条日志的 term。
     */
    private long lastIncludedTerm;

    /**
     * 快照 JSON 文本（{@code SnapshotData} 序列化）。
     */
    private String snapshot;

    public SnapshotRequest() {
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(long lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }
}
