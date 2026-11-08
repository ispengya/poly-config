package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ForwardReadRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.raft.RaftProtocol;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * CP 线性读处理器（RAFT_READ，4005，任意节点 → leader）。
 *
 * <p>leader 侧完成 read-index（基线 + 心跳轮确认 + 等 apply）后读本地
 * 状态机，应答携带 {@link ConfigView}；key 不存在回 NOT_FOUND。本节点
 * 非 leader 时由 readIndex 角色闸拒绝（UNAVAILABLE），转发方按错误语义
 * 重试——与 {@link RaftForwardProcessor} 同款"不做二次转发"策略。</p>
 */
public class RaftReadProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RaftReadProcessor.class);

    private final RaftProtocol protocol;

    /**
     * @param protocol CP 协议（leader 侧线性读入口）
     */
    public RaftReadProcessor(RaftProtocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "线性读请求体为空");
            }
            ForwardReadRequest forward = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    ForwardReadRequest.class);
            if (forward == null || forward.getDataId() == null) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "线性读请求体非法");
            }
            ConfigKey key = new ConfigKey(forward.getNamespace(), forward.getGroup(),
                    forward.getDataId());
            Optional<ConfigEntry> entry = protocol.readLocalLinearizable(key);
            if (!entry.isPresent()) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_NOT_FOUND, "配置不存在: " + key.toPath());
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(ConfigView.from(entry.get()))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            log.warn("CP 线性读处理失败: {}", e.getMessage());
            return RemotingCommand.createResponseCommand(
                    e.getErrorCode() == ErrorCode.NOT_FOUND
                            ? ProtocolCodes.RESPONSE_NOT_FOUND
                            : ProtocolCodes.RESPONSE_SERVER_ERROR,
                    e.getMessage());
        } catch (RuntimeException e) {
            log.error("CP 线性读处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "线性读处理失败");
        }
    }
}
