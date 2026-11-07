package com.ispengya.polyconfig.core.raft;

import java.util.Arrays;

/**
 * 一条 Raft 日志：命令（payload）+ 身份证（index/term）。
 *
 * <p>{@code index} 是日志在序列中的位置编号（全集群同号必同内容），
 * {@code term} 是产生该条目的 leader 任期。payload 为命令编码字节
 * （{@code ConfigCommand} 的 CommandCodec 编码），RaftLog 不解析其内容。</p>
 */
public final class RaftLogEntry {

    private final long index;

    private final long term;

    private final byte[] payload;

    public RaftLogEntry(long index, long term, byte[] payload) {
        if (index < 1) {
            throw new IllegalArgumentException("index 必须 ≥ 1: " + index);
        }
        if (term < 1) {
            throw new IllegalArgumentException("term 必须 ≥ 1: " + term);
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload 不能为空");
        }
        this.index = index;
        this.term = term;
        this.payload = payload;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public byte[] getPayload() {
        return payload;
    }

    /**
     * 返回 payload 的拷贝，避免调用方持有内部数组引用。
     */
    public byte[] copyPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    @Override
    public String toString() {
        return "RaftLogEntry{index=" + index + ", term=" + term
                + ", payloadBytes=" + payload.length + '}';
    }
}
