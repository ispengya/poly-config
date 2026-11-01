package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SubscribeRequest;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.server.push.SubscriptionRegistry;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 取消订阅请求处理器。
 *
 * <p>请求体复用 {@link SubscribeRequest}，只需 namespace/group/dataId 定位配置键。</p>
 */
public class UnsubscribeProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(UnsubscribeProcessor.class);

    /**
     * 订阅注册表。
     */
    private final SubscriptionRegistry registry;

    /**
     * 创建取消订阅处理器。
     *
     * @param registry 订阅注册表
     */
    public UnsubscribeProcessor(SubscriptionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "取消订阅请求体为空");
            }
            SubscribeRequest unsubscribe = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8), SubscribeRequest.class);
            ConfigKey key = new ConfigKey(unsubscribe.getNamespace(), unsubscribe.getGroup(),
                    unsubscribe.getDataId());
            registry.unsubscribe(ctx.channel(), key);
            return RemotingCommand.createResponseCommand(ProtocolCodes.RESPONSE_OK, "ok");
        } catch (PolyConfigException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            log.error("取消订阅请求处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "取消订阅处理失败");
        }
    }
}
