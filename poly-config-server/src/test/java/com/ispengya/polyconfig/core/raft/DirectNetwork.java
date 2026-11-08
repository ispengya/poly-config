package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.SnapshotRequest;
import com.ispengya.polyconfig.common.protocol.SnapshotResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 进程内直连网络（测试用）：按 nodeId 把 RPC 直投目标节点的单线程信箱。
 *
 * <p>目标未注册（或已关闭导致任务丢弃）时回调 {@code null}——等价于
 * 网络不可达，与 remoting 传输的失败语义一致。</p>
 *
 * <p>支持节点级双向隔离（{@link #isolate}/{@link #heal}）：被隔离节点的
 * 出站与入站 RPC 全部丢弃，模拟分区——图 8 用例需要"leader 孤立但其余
 * 节点互联"的非对称场景。隔离检查依赖发送方视图（{@link #viewOf}），
 * 构造 RaftNode 时须传入绑定本节点的视图。</p>
 */
final class DirectNetwork {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

    /**
     * 被双向隔离的节点（出站 + 入站 RPC 全部丢弃）。
     */
    private final Set<String> isolated = ConcurrentHashMap.newKeySet();

    /**
     * 注册节点（构造后调用）。
     */
    void register(String nodeId, RaftNode node) {
        nodes.put(nodeId, node);
    }

    /**
     * 摘除节点（模拟宕机：后续发送静默失败）。
     */
    void unregister(String nodeId) {
        nodes.remove(nodeId);
    }

    /**
     * 双向隔离节点（模拟分区：出站与入站 RPC 全部丢弃）。
     */
    void isolate(String nodeId) {
        isolated.add(nodeId);
    }

    /**
     * 解除隔离。
     */
    void heal(String nodeId) {
        isolated.remove(nodeId);
    }

    /**
     * 返回绑定发送方的传输视图：出站经隔离检查后投递。
     *
     * @param senderId 发送方节点 ID
     * @return 该节点专用的传输视图
     */
    RaftTransport viewOf(String senderId) {
        return new RaftTransport() {
            @Override
            public void sendVote(ClusterMember peer, VoteRequest request,
                                 Consumer<VoteResponse> callback) {
                if (isolated.contains(senderId) || isolated.contains(peer.getNodeId())) {
                    callback.accept(null);
                    return;
                }
                deliverVote(peer, request, callback);
            }

            @Override
            public void sendAppend(ClusterMember peer, AppendEntriesRequest request,
                                   Consumer<AppendEntriesResponse> callback) {
                if (isolated.contains(senderId) || isolated.contains(peer.getNodeId())) {
                    callback.accept(null);
                    return;
                }
                deliverAppend(peer, request, callback);
            }

            @Override
            public void sendSnapshot(ClusterMember peer, SnapshotRequest request,
                                     Consumer<SnapshotResponse> callback) {
                if (isolated.contains(senderId) || isolated.contains(peer.getNodeId())) {
                    callback.accept(null);
                    return;
                }
                deliverSnapshot(peer, request, callback);
            }
        };
    }

    private void deliverVote(ClusterMember peer, VoteRequest request,
                             Consumer<VoteResponse> callback) {
        RaftNode target = nodes.get(peer.getNodeId());
        if (target == null) {
            callback.accept(null);
            return;
        }
        target.submitVoteRequest(request).whenComplete((response, error) ->
                callback.accept(error == null ? response : null));
    }

    private void deliverAppend(ClusterMember peer, AppendEntriesRequest request,
                               Consumer<AppendEntriesResponse> callback) {
        RaftNode target = nodes.get(peer.getNodeId());
        if (target == null) {
            callback.accept(null);
            return;
        }
        target.submitAppendRequest(request).whenComplete((response, error) ->
                callback.accept(error == null ? response : null));
    }

    private void deliverSnapshot(ClusterMember peer, SnapshotRequest request,
                                 Consumer<SnapshotResponse> callback) {
        RaftNode target = nodes.get(peer.getNodeId());
        if (target == null) {
            callback.accept(null);
            return;
        }
        target.submitInstallSnapshotRequest(request).whenComplete((response, error) ->
                callback.accept(error == null ? response : null));
    }
}
