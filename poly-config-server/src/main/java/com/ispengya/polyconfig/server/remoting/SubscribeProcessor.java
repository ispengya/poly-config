package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SubscribeRequest;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.polyconfig.server.push.SubscriptionRegistry;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * 订阅请求处理器。
 */
public class SubscribeProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(SubscribeProcessor.class);

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 订阅注册表。
     */
    private final SubscriptionRegistry registry;

    /**
     * 创建订阅处理器。
     *
     * @param configService 配置服务
     * @param registry 订阅注册表
     */
    public SubscribeProcessor(ConfigService configService, SubscriptionRegistry registry) {
        this.configService = configService;
        this.registry = registry;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "订阅请求体为空");
            }
            SubscribeRequest subscribe = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8), SubscribeRequest.class);
            ConfigKey key = new ConfigKey(subscribe.getNamespace(), subscribe.getGroup(),
                    subscribe.getDataId());
            registry.subscribe(ctx.channel(), key);

            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            Optional<ConfigEntry> entry = configService.get(key.getNamespace(),
                    key.getGroup(), key.getDataId());
            if (entry.isPresent()) {
                String requestedMd5 = subscribe.getMd5();
                if (requestedMd5 == null || requestedMd5.isEmpty()
                        || !requestedMd5.equals(entry.get().getMd5())) {
                    response.setBody(JsonUtils.toJson(ConfigView.from(entry.get()))
                            .getBytes(StandardCharsets.UTF_8));
                }
            }
            return response;
        } catch (PolyConfigException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            log.error("订阅请求处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "订阅处理失败");
        }
    }
}
