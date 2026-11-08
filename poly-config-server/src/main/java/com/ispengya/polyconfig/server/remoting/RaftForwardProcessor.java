package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.ForwardWriteRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteResult;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.raft.RaftProtocol;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * CP 写转发处理器（RAFT_FORWARD，4004，leader 侧）。
 *
 * <p>接收任意节点转发来的写请求，在本机走 {@link RaftProtocol#proposeLocal}
 * （D5 校验 expectedVersion、赋版本、追加日志、等 commit+apply），应答携带
 * 分配的 version 与命令回显，供转发节点按该 version 提前 apply（写后即读）。</p>
 *
 * <p>只在 mode=cp 装配时注册；本节点非 leader 时由 proposeLocal 的角色闸
 * 拒绝（应答带当前 leader 线索），不做二次转发——转发方按错误语义重试。</p>
 */
public class RaftForwardProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RaftForwardProcessor.class);

    private final RaftProtocol protocol;

    /**
     * @param protocol CP 协议（leader 本机写入口）
     */
    public RaftForwardProcessor(RaftProtocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "转发请求体为空");
            }
            ForwardWriteRequest forward = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    ForwardWriteRequest.class);
            if (forward == null || forward.getDataId() == null) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "转发请求体非法");
            }
            ConfigCommand command = forward.toCommand();
            MutationResult result = protocol.proposeLocal(command);
            log.info("应用 CP 转发写入, key={}, version={}, operator={}",
                    command.getKey().toPath(), result.getVersion(),
                    command.getOperator());
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(ForwardWriteResult.from(command, result))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            log.warn("CP 写转发处理失败: {}", e.getMessage());
            return RemotingCommand.createResponseCommand(
                    toResponseCode(e.getErrorCode()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("CP 写转发处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "写转发处理失败");
        }
    }

    private static int toResponseCode(ErrorCode errorCode) {
        switch (errorCode) {
            case VERSION_CONFLICT:
                return ProtocolCodes.RESPONSE_VERSION_CONFLICT;
            case NOT_FOUND:
                return ProtocolCodes.RESPONSE_NOT_FOUND;
            case INVALID_ARGUMENT:
                return ProtocolCodes.RESPONSE_BAD_REQUEST;
            default:
                return ProtocolCodes.RESPONSE_SERVER_ERROR;
        }
    }
}
