package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 进程内直连网络（测试用）：按 nodeId 把 RPC 直投目标节点的单线程信箱。
 *
 * <p>目标未注册（或已关闭导致任务丢弃）时回调 {@code null}——等价于
 * 网络不可达，与 remoting 传输的失败语义一致。</p>
 */
final class DirectNetwork implements RaftTransport {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

    /**
     * 注册节点（构造后调用）。
     */
    void register(String nodeId, RaftNode node) {
        nodes.put(nodeId, node);
    }

    /**
     * 摘除节点（模拟宕机/分区：后续发送静默失败）。
     */
    void unregister(String nodeId) {
        nodes.remove(nodeId);
    }

    @Override
    public void sendVote(ClusterMember peer, VoteRequest request,
                         Consumer<VoteResponse> callback) {
        RaftNode target = nodes.get(peer.getNodeId());
        if (target == null) {
            callback.accept(null);
            return;
        }
        target.submitVoteRequest(request).whenComplete((response, error) ->
                callback.accept(error == null ? response : null));
    }

    @Override
    public void sendAppend(ClusterMember peer, AppendEntriesRequest request,
                           Consumer<AppendEntriesResponse> callback) {
        RaftNode target = nodes.get(peer.getNodeId());
        if (target == null) {
            callback.accept(null);
            return;
        }
        target.submitAppendRequest(request).whenComplete((response, error) ->
                callback.accept(error == null ? response : null));
    }
}
