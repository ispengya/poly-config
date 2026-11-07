package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
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
 * CP 复制/心跳处理器（RAFT_APPEND，4xxx 内部协议）。
 *
 * <p>与 {@link RaftVoteProcessor} 同构：编解码 → 投递单线程信箱 → 等待
 * 应答（闸1 term 比对、闸2 接缝核对在状态线程内完成，心跳的选举计时器
 * 重置同样发生在那里）。</p>
 */
public class RaftAppendProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RaftAppendProcessor.class);

    /**
     * 等待状态线程处理的超时。
     */
    private static final long HANDLE_TIMEOUT_SECONDS = 2L;

    private final RaftNode raftNode;

    /**
     * @param raftNode Raft 节点
     */
    public RaftAppendProcessor(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        if (request.getBody() == null || request.getBody().length == 0) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "复制请求体为空");
        }
        AppendEntriesRequest appendRequest;
        try {
            appendRequest = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    AppendEntriesRequest.class);
        } catch (RuntimeException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "复制请求解析失败");
        }
        if (appendRequest == null || appendRequest.getLeaderId() == null) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "复制请求缺少 leaderId");
        }
        try {
            AppendEntriesResponse response = raftNode.submitAppendRequest(appendRequest)
                    .get(HANDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RemotingCommand result = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            result.setBody(JsonUtils.toJson(response).getBytes(StandardCharsets.UTF_8));
            return result;
        } catch (TimeoutException e) {
            log.warn("复制请求处理超时: leader={}", appendRequest.getLeaderId());
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "状态机线程处理超时");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "处理被中断");
        } catch (Exception e) {
            log.error("复制请求处理失败: leader={}", appendRequest.getLeaderId(), e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "复制请求处理失败");
        }
    }
}
