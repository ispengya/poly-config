package com.ispengya.polyconfig.common.protocol;

/**
 * Raft 复制/心跳应答。
 *
 * <p>{@code success=false} 时 {@code lastLogIndex/lastLogTerm} 回带
 * 接收侧日志尾线索，供 leader 跳跃式回退 nextIndex（CP-3 起使用）。</p>
 */
public class AppendEntriesResponse {

    /**
     * 应答方当前任期（供 leader 追平/退位）。
     */
    private long term;

    /**
     * 闸1+闸2 是否通过。
     */
    private boolean success;

    /**
     * 应答方最后一条日志的 index（拒绝时的回退线索）。
     */
    private long lastLogIndex;

    /**
     * 应答方最后一条日志的 term。
     */
    private long lastLogTerm;

    /**
     * 本条消息确认已对齐到的 index（成功为 prevLogIndex，失败为 -1）。
     */
    private long matchIndex = -1L;

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

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }
}
