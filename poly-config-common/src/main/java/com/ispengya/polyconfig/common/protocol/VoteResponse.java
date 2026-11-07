package com.ispengya.polyconfig.common.protocol;

/**
 * Raft 拉票应答。
 *
 * <p>{@code term} 用于规则一：候选人发现更高 term 立即回退；
 * {@code voteGranted} 表示是否投出本 term 的选票。</p>
 */
public class VoteResponse {

    /**
     * 应答方当前任期（供候选人追平）。
     */
    private long term;

    /**
     * 是否投赞成票。
     */
    private boolean voteGranted;

    public VoteResponse() {
    }

    public VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
