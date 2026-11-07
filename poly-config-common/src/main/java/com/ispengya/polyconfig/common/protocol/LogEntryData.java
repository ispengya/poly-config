package com.ispengya.polyconfig.common.protocol;

/**
 * AppendEntries 消息中携带的一条日志（wire 形态）。
 *
 * <p>{@code payload} 为命令编码字节的 Base64（JSON 安全）；
 * {@code index/term} 随条目携带，便于接收侧校验连续性。</p>
 */
public class LogEntryData {

    /**
     * 日志 index。
     */
    private long index;

    /**
     * 日志 term。
     */
    private long term;

    /**
     * 命令编码字节（Base64）。
     */
    private String payload;

    public LogEntryData() {
    }

    public LogEntryData(long index, long term, String payload) {
        this.index = index;
        this.term = term;
        this.payload = payload;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
