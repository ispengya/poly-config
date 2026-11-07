package com.ispengya.polyconfig.common.protocol;

/**
 * Raft 拉票请求（RAFT_VOTE，4xxx 内部协议）。
 *
 * <p>候选人日志尾信息用于投票闸门：只有"够新"的候选人才能获得选票
 * （选举安全条件）。</p>
 */
public class VoteRequest {

    /**
     * 候选人任期。
     */
    private long term;

    /**
     * 候选人节点 ID。
     */
    private String candidateId;

    /**
     * 候选人最后一条日志的 index。
     */
    private long lastLogIndex;

    /**
     * 候选人最后一条日志的 term。
     */
    private long lastLogTerm;

    public VoteRequest() {
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
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
