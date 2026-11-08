package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.storage.snapshot.SnapshotData;

/**
 * Raft 节点对快照存储的访问口（cp-design.md §7/§10：零件复用、编排在 RaftNode）。
 *
 * <p>由 {@code RaftProtocol} 实现：快照文件（{@code SnapshotStore}）与状态机
 * 恢复归协议层，RaftNode 只负责快照位点（lastIncludedIndex/Term）、日志
 * 压缩与 InstallSnapshot 的收发时序。方法在状态线程上调用。</p>
 */
public interface RaftSnapshotAccess {

    /**
     * 当前已持久化的最新快照（供 InstallSnapshot 下发），无则 {@code null}。
     *
     * @return 快照内容
     */
    SnapshotData currentSnapshot();

    /**
     * 安装一份快照到本节点：先原子持久化文件，成功后再恢复状态机
     * （顺序保证任一时刻崩溃都可由"文件 + 日志"确定性恢复）。
     *
     * <p>失败抛异常——调用方（RaftNode）回拒绝应答，不产生半截状态。</p>
     *
     * @param data 快照内容（含 lastIncludedIndex/Term）
     */
    void installSnapshot(SnapshotData data);
}
