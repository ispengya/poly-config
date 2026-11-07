package com.ispengya.polyconfig.common.protocol;

import java.util.List;

/**
 * Raft 复制/心跳请求（RAFT_APPEND，4xxx 内部协议）。
 *
 * <p>{@code entries} 为空即心跳。{@code prevLogIndex/prevLogTerm} 是
 * 接缝凭据：接收侧核对本地同号条目的 term，一致才允许拼接（闸2）。</p>
 */
public class AppendEntriesRequest {

    /**
     * leader 任期。
     */
    private long term;

    /**
     * leader 节点 ID。
     */
    private String leaderId;

    /**
     * 新条目前一条的 index（0 表示从头开始）。
     */
    private long prevLogIndex;

    /**
     * 新条目前一条的 term。
     */
    private long prevLogTerm;

    /**
     * leader 已提交到的 index。
     */
    private long leaderCommit;

    /**
     * 要复制的条目；空列表为心跳。
     */
    private List<LogEntryData> entries;

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

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public List<LogEntryData> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntryData> entries) {
        this.entries = entries;
    }
}
