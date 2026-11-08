package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SnapshotRequest;
import com.ispengya.polyconfig.common.protocol.SnapshotResponse;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.raft.RaftNode;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CP 快照下发处理器（RAFT_SNAPSHOT，4003，leader → 落后 follower）。
 *
 * <p>与 {@link RaftAppendProcessor} 同构：编解码 → 投递单线程信箱 → 等待
 * 应答（term 闸、已有覆盖判定、先持久化再丢日志的安装时序都在状态线程内
 * 完成）。安装含快照文件原子写，等待上限放宽到 5 秒。</p>
 */
public class RaftSnapshotProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RaftSnapshotProcessor.class);

    /**
     * 等待状态线程处理的超时（含快照文件落盘）。
     */
    private static final long HANDLE_TIMEOUT_SECONDS = 5L;

    private final RaftNode raftNode;

    /**
     * @param raftNode Raft 节点
     */
    public RaftSnapshotProcessor(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        if (request.getBody() == null || request.getBody().length == 0) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "快照请求体为空");
        }
        SnapshotRequest snapshotRequest;
        try {
            snapshotRequest = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    SnapshotRequest.class);
        } catch (RuntimeException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "快照请求解析失败");
        }
        if (snapshotRequest == null || snapshotRequest.getLeaderId() == null
                || snapshotRequest.getSnapshot() == null) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "快照请求缺少 leaderId/snapshot");
        }
        try {
            SnapshotResponse response = raftNode.submitInstallSnapshotRequest(snapshotRequest)
                    .get(HANDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RemotingCommand result = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            result.setBody(JsonUtils.toJson(response).getBytes(StandardCharsets.UTF_8));
            return result;
        } catch (TimeoutException e) {
            log.warn("快照安装处理超时: leader={}", snapshotRequest.getLeaderId());
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "状态机线程处理超时");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "处理被中断");
        } catch (Exception e) {
            log.error("快照安装处理失败: leader={}", snapshotRequest.getLeaderId(), e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "快照安装处理失败");
        }
    }
}
