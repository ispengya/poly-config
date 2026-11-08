package com.ispengya.polyconfig.common.protocol;

/**
 * CP InstallSnapshot 应答（RAFT_SNAPSHOT）。
 *
 * <p>成功时 {@code matchIndex} 回带接收方安装后的覆盖位点（= lastIncludedIndex
 * 或接收方已有的更高位点），leader 据此校准 nextIndex 继续补日志；
 * term 回带接收方当前任期，供过期 leader 退位（规则一）。</p>
 */
public class SnapshotResponse {

    /**
     * 应答方当前任期。
     */
    private long term;

    /**
     * 是否接受本份快照（或已有同等/更新状态）。
     */
    private boolean success;

    /**
     * 应答方确认已覆盖到的 index（成功为 max(快照位点, 已有覆盖)，失败为 -1）。
     */
    private long matchIndex = -1L;

    /**
     * 应答方日志尾 index（诊断/线索用）。
     */
    private long lastLogIndex;

    /**
     * 应答方日志尾 term。
     */
    private long lastLogTerm;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }
}
