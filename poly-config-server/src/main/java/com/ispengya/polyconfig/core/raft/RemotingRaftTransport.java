package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SnapshotRequest;
import com.ispengya.polyconfig.common.protocol.SnapshotResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 基于 remoting 的 Raft 传输：复用长连接端口（4xxx 内部协议）。
 *
 * <p>发送在独立线程池上同步 invokeSync，不阻塞状态机线程；应答解析后经
 * 回调交还 {@link RaftNode}（其内部再投递回单线程信箱）。失败（超时、
 * 连接拒绝、非 OK 应答）统一回调 {@code null}，由节点按"下一轮再试"处理
 * ——与 AP"复制失败不重试、周期补齐"的容错哲学一致。</p>
 */
final class RemotingRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(RemotingRaftTransport.class);

    private final NettyRemotingClient client;

    private final long rpcTimeoutMillis;

    private final ExecutorService sendPool;

    /**
     * @param client 节点间调用客户端
     * @param rpcTimeoutMillis 单次 RPC 超时
     * @param threads 发送线程数（建议 ≥ 对等节点数）
     */
    RemotingRaftTransport(NettyRemotingClient client, long rpcTimeoutMillis, int threads) {
        this.client = client;
        this.rpcTimeoutMillis = rpcTimeoutMillis;
        this.sendPool = Executors.newFixedThreadPool(Math.max(2, threads), runnable -> {
            Thread thread = new Thread(runnable, "poly-config-raft-sender");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void sendVote(ClusterMember peer, VoteRequest request,
                         Consumer<VoteResponse> callback) {
        sendPool.execute(() -> {
            VoteResponse response = null;
            try {
                RemotingCommand rpc = RemotingCommand.createRequestCommand(
                        ProtocolCodes.RAFT_VOTE, null);
                rpc.setBody(JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8));
                RemotingCommand result = client.invokeSync(address(peer), rpc, rpcTimeoutMillis);
                if (result.getCode() == ProtocolCodes.RESPONSE_OK
                        && result.getBody() != null && result.getBody().length > 0) {
                    response = JsonUtils.parseObject(
                            new String(result.getBody(), StandardCharsets.UTF_8),
                            VoteResponse.class);
                } else {
                    log.debug("拉票被拒绝/异常: peer={}, code={}", peer.getNodeId(),
                            result.getCode());
                }
            } catch (Exception e) {
                log.debug("拉票发送失败: peer={}: {}", peer.getNodeId(), e.toString());
            }
            callback.accept(response);
        });
    }

    @Override
    public void sendAppend(ClusterMember peer, AppendEntriesRequest request,
                           Consumer<AppendEntriesResponse> callback) {
        sendPool.execute(() -> {
            AppendEntriesResponse response = null;
            try {
                RemotingCommand rpc = RemotingCommand.createRequestCommand(
                        ProtocolCodes.RAFT_APPEND, null);
                rpc.setBody(JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8));
                RemotingCommand result = client.invokeSync(address(peer), rpc, rpcTimeoutMillis);
                if (result.getCode() == ProtocolCodes.RESPONSE_OK
                        && result.getBody() != null && result.getBody().length > 0) {
                    response = JsonUtils.parseObject(
                            new String(result.getBody(), StandardCharsets.UTF_8),
                            AppendEntriesResponse.class);
                } else {
                    log.debug("复制/心跳被拒绝/异常: peer={}, code={}", peer.getNodeId(),
                            result.getCode());
                }
            } catch (Exception e) {
                log.debug("复制/心跳发送失败: peer={}: {}", peer.getNodeId(), e.toString());
            }
            callback.accept(response);
        });
    }

    @Override
    public void sendSnapshot(ClusterMember peer, SnapshotRequest request,
                             Consumer<SnapshotResponse> callback) {
        sendPool.execute(() -> {
            SnapshotResponse response = null;
            try {
                RemotingCommand rpc = RemotingCommand.createRequestCommand(
                        ProtocolCodes.RAFT_SNAPSHOT, null);
                rpc.setBody(JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8));
                RemotingCommand result = client.invokeSync(address(peer), rpc, rpcTimeoutMillis);
                if (result.getCode() == ProtocolCodes.RESPONSE_OK
                        && result.getBody() != null && result.getBody().length > 0) {
                    response = JsonUtils.parseObject(
                            new String(result.getBody(), StandardCharsets.UTF_8),
                            SnapshotResponse.class);
                } else {
                    log.debug("快照下发被拒绝/异常: peer={}, code={}", peer.getNodeId(),
                            result.getCode());
                }
            } catch (Exception e) {
                log.debug("快照下发发送失败: peer={}: {}", peer.getNodeId(), e.toString());
            }
            callback.accept(response);
        });
    }

    /**
     * 关闭发送线程池。
     */
    void close() {
        sendPool.shutdownNow();
        try {
            sendPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String address(ClusterMember member) {
        return member.getHost() + ":" + member.getRemotingPort();
    }
}
