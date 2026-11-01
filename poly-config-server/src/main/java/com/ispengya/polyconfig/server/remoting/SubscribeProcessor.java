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
            // 单连接订阅数上限
            if (!registry.subscribe(ctx.channel(), key)) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST,
                        "订阅数超出单连接上限: " + key);
            }

            Optional<ConfigEntry> entry = configService.get(key.getNamespace(),
                    key.getGroup(), key.getDataId());
            if (!entry.isPresent()) {
                // 客户端声明持有版本（revision>0 或 md5 非空）但服务端已无此键，说明已删除；
                // 首次订阅（两者皆空）不存在可删内容，按"暂无配置"成功返回
                if (subscribe.getRevision() > 0
                        || (subscribe.getMd5() != null && !subscribe.getMd5().isEmpty())) {
                    return RemotingCommand.createResponseCommand(
                            ProtocolCodes.RESPONSE_NOT_FOUND, "配置不存在");
                }
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_OK, "ok");
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            if (isUnchanged(subscribe.getRevision(), subscribe.getMd5(), entry.get())) {
                return response;
            }
            response.setBody(JsonUtils.toJson(ConfigView.from(entry.get()))
                    .getBytes(StandardCharsets.UTF_8));
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

    /**
     * 判断客户端声明的版本与服务端当前条目一致。
     *
     * @param clientRevision 客户端持有的 revision，0 表示未知
     * @param clientMd5 客户端持有的 MD5，空表示未知
     * @param entry 服务端当前条目
     * @return 内容未变更返回 {@code true}
     */
    static boolean isUnchanged(long clientRevision, String clientMd5, ConfigEntry entry) {
        if (clientRevision > 0 && clientRevision == entry.getRevision()) {
            return true;
        }
        return clientMd5 != null && !clientMd5.isEmpty()
                && clientMd5.equals(entry.getMd5());
    }
}
