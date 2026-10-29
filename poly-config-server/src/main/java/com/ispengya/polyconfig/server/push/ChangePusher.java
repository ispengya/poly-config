package com.ispengya.polyconfig.server.push;

import com.ispengya.polyconfig.common.model.ConfigChangeEvent;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.PushEvent;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.remoting.RemotingServer;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 把状态机变更推送给订阅该配置的客户端。
 */
public class ChangePusher implements ChangeListener {

    private static final Logger log = LoggerFactory.getLogger(ChangePusher.class);

    /**
     * 服务端推送通道。
     */
    private final RemotingServer remotingServer;

    /**
     * 订阅注册表。
     */
    private final SubscriptionRegistry registry;

    /**
     * 创建变更推送器。
     *
     * @param remotingServer 服务端推送通道
     * @param registry 订阅注册表
     */
    public ChangePusher(RemotingServer remotingServer, SubscriptionRegistry registry) {
        this.remotingServer = remotingServer;
        this.registry = registry;
    }

    @Override
    public void onChange(ConfigChangeEvent event) {
        List<Channel> channels = registry.channels(event.getKey());
        if (channels.isEmpty()) {
            return;
        }
        byte[] body = JsonUtils.toJson(PushEvent.from(event))
                .getBytes(StandardCharsets.UTF_8);
        for (Channel channel : channels) {
            if (!channel.isActive()) {
                registry.remove(channel);
                continue;
            }
            RemotingCommand push = RemotingCommand.createPushCommand(
                    ProtocolCodes.PUSH_CONFIG_CHANGED);
            push.setBody(body);
            try {
                ChannelFuture future = remotingServer.push(channel, push);
                future.addListener(result -> {
                    if (!result.isSuccess()) {
                        log.warn("配置推送失败, key={}, channel={}", event.getKey(), channel);
                        registry.remove(channel);
                    }
                });
            } catch (RuntimeException e) {
                log.warn("配置推送异常, key={}, channel={}", event.getKey(), channel, e);
                registry.remove(channel);
            }
        }
    }
}
