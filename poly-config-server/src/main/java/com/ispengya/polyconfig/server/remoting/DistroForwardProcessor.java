package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ForwardReadRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteRequest;
import com.ispengya.polyconfig.common.protocol.ForwardWriteResult;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * AP 节点间转发处理器（owner 侧，3xxx 内部协议）。
 *
 * <p>{@code DISTRO_FORWARD}：接收任意节点转发来的写请求，在 owner 本机
 * {@code proposeLocal}（校验 expectedVersion、分配版本、落盘），应答中携带
 * owner 分配的 version 与命令回显，供转发节点同步 apply。</p>
 *
 * <p>{@code DISTRO_READ}：线性读，读 owner 本地状态并以 {@code ConfigView} 应答。</p>
 *
 * <p>只在 mode=ap 装配时注册；直接操作引擎而非 ConfigService，保证走的是
 * owner 本地写入口而不是再次路由。</p>
 */
public class DistroForwardProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(DistroForwardProcessor.class);

    /**
     * 本地存储引擎（owner 本机）。
     */
    private final LocalEngine engine;

    /**
     * 创建转发处理器。
     *
     * @param engine 本地存储引擎
     */
    public DistroForwardProcessor(LocalEngine engine) {
        this.engine = engine;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        if (request.getCode() == ProtocolCodes.DISTRO_READ) {
            return handleRead(request);
        }
        return handleWrite(request);
    }

    private RemotingCommand handleWrite(RemotingCommand request) {
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
            MutationResult result = engine.proposeLocal(command);
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(ForwardWriteResult.from(command, result))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            log.warn("写转发处理失败: {}", e.getMessage());
            return RemotingCommand.createResponseCommand(
                    toResponseCode(e.getErrorCode()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("写转发处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "写转发处理失败");
        }
    }

    private RemotingCommand handleRead(RemotingCommand request) {
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
            Optional<ConfigEntry> entry = engine.read(key);
            if (!entry.isPresent()) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_NOT_FOUND, "配置不存在");
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(ConfigView.from(entry.get()))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            return RemotingCommand.createResponseCommand(
                    toResponseCode(e.getErrorCode()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("线性读处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "线性读处理失败");
        }
    }

    private static int toResponseCode(ErrorCode errorCode) {
        switch (errorCode) {
            case VERSION_CONFLICT:
                return ProtocolCodes.RESPONSE_VERSION_CONFLICT;
            case NOT_FOUND:
                return ProtocolCodes.RESPONSE_NOT_FOUND;
            case INVALID_ARGUMENT:
            case PAYLOAD_TOO_LARGE:
                return ProtocolCodes.RESPONSE_BAD_REQUEST;
            default:
                return ProtocolCodes.RESPONSE_SERVER_ERROR;
        }
    }
}
