package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;

import java.util.function.Consumer;

/**
 * Raft 节点间 RPC 传输。
 *
 * <p>全部异步：发送方不阻塞状态机线程，应答（或失败 {@code null}）经回调
 * 送达，由 {@link RaftNode} 自行提交回单线程信箱——回调可能来自任意线程。</p>
 */
public interface RaftTransport {

    /**
     * 异步发送拉票请求。
     *
     * @param peer 目标成员
     * @param request 请求
     * @param callback 应答回调；失败传 {@code null}
     */
    void sendVote(ClusterMember peer, VoteRequest request, Consumer<VoteResponse> callback);

    /**
     * 异步发送复制/心跳请求。
     *
     * @param peer 目标成员
     * @param request 请求
     * @param callback 应答回调；失败传 {@code null}
     */
    void sendAppend(ClusterMember peer, AppendEntriesRequest request,
                    Consumer<AppendEntriesResponse> callback);
}
