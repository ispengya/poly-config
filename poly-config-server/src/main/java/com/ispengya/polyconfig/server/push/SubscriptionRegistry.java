package com.ispengya.polyconfig.server.push;

import com.ispengya.polyconfig.common.model.ConfigKey;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端订阅注册表。
 *
 * <p>同时维护 key → channel 与 channel → key 两张索引，连接关闭时统一清理。</p>
 */
public class SubscriptionRegistry {

    /**
     * 配置键到连接集合。
     */
    private final ConcurrentHashMap<ConfigKey, Set<Channel>> channelsByKey =
            new ConcurrentHashMap<>();

    /**
     * 连接到配置键集合。
     */
    private final ConcurrentHashMap<Channel, Set<ConfigKey>> keysByChannel =
            new ConcurrentHashMap<>();

    /**
     * 已注册关闭监听的连接。
     */
    private final Set<Channel> listeningChannels = ConcurrentHashMap.newKeySet();

    /**
     * 注册一个连接对配置键的订阅。
     *
     * @param channel 客户端连接
     * @param key 配置键
     */
    public void subscribe(Channel channel, ConfigKey key) {
        keysByChannel.computeIfAbsent(channel, value -> ConcurrentHashMap.newKeySet()).add(key);
        channelsByKey.computeIfAbsent(key, value -> ConcurrentHashMap.newKeySet()).add(channel);
        if (listeningChannels.add(channel)) {
            channel.closeFuture().addListener(future -> remove(channel));
        }
    }

    /**
     * 返回订阅了指定配置键的连接快照。
     *
     * @param key 配置键
     * @return 连接列表
     */
    public List<Channel> channels(ConfigKey key) {
        Set<Channel> channels = channelsByKey.get(key);
        if (channels == null || channels.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(channels);
    }

    /**
     * 移除连接的全部订阅。
     *
     * @param channel 客户端连接
     */
    public void remove(Channel channel) {
        listeningChannels.remove(channel);
        Set<ConfigKey> keys = keysByChannel.remove(channel);
        if (keys == null) {
            return;
        }
        for (ConfigKey key : keys) {
            Set<Channel> channels = channelsByKey.get(key);
            if (channels == null) {
                continue;
            }
            channels.remove(channel);
            if (channels.isEmpty()) {
                channelsByKey.remove(key, channels);
            }
        }
    }
}
