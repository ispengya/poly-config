package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
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
 * CP 拉票处理器（RAFT_VOTE，4xxx 内部协议）。
 *
 * <p>网络线程只做编解码与投递：消息进 {@link RaftNode} 单线程信箱，
 * 等待状态线程完成（含 term/vote fsync 落盘）后应答——"先落盘再投票"
 * 的时序由该等待保证。状态线程超时兜底 2 秒，避免慢盘拖死 remoting 线程。</p>
 */
public class RaftVoteProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RaftVoteProcessor.class);

    /**
     * 等待状态线程处理的超时。
     */
    private static final long HANDLE_TIMEOUT_SECONDS = 2L;

    private final RaftNode raftNode;

    /**
     * @param raftNode Raft 节点
     */
    public RaftVoteProcessor(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        if (request.getBody() == null || request.getBody().length == 0) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "拉票请求体为空");
        }
        VoteRequest voteRequest;
        try {
            voteRequest = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8), VoteRequest.class);
        } catch (RuntimeException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "拉票请求解析失败");
        }
        if (voteRequest == null || voteRequest.getCandidateId() == null) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, "拉票请求缺少 candidateId");
        }
        try {
            VoteResponse response = raftNode.submitVoteRequest(voteRequest)
                    .get(HANDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RemotingCommand result = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            result.setBody(JsonUtils.toJson(response).getBytes(StandardCharsets.UTF_8));
            return result;
        } catch (TimeoutException e) {
            log.warn("拉票处理超时: candidate={}", voteRequest.getCandidateId());
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "状态机线程处理超时");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "处理被中断");
        } catch (Exception e) {
            log.error("拉票处理失败: candidate={}", voteRequest.getCandidateId(), e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "拉票处理失败");
        }
    }
}
