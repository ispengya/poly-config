package com.ispengya.polyconfig.core.raft;

/**
 * 已提交条目的 apply 回调（状态线程调用）。
 *
 * <p>no-op 空载荷不回调。CP 实现由 {@code RaftProtocol} 提供：解码命令并
 * 应用到 {@code ConfigStateMachine}（revision = index，version = 命令携带的
 * assignedVersion）。</p>
 */
public interface RaftApplyListener {

    /**
     * 应用一条已提交条目到状态机。
     *
     * @param index 条目 index（同时是节点本地 revision）
     * @param term 条目 term
     * @param payload 命令编码字节
     */
    void onApply(long index, long term, byte[] payload);
}
