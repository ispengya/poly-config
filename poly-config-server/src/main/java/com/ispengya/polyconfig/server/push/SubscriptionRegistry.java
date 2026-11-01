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
 * <p>同时维护 key → channel 与 channel → key 两张索引，连接关闭时统一清理；
 * 支持单键取消订阅，并对单连接订阅数设上限。</p>
 */
public class SubscriptionRegistry {

    /**
     * 单连接最大订阅数，0 或负数表示不限制。
     */
    private final int maxSubscriptionsPerConnection;

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
     * 创建默认上限的订阅注册表。
     */
    public SubscriptionRegistry() {
        this(1024);
    }

    /**
     * 创建订阅注册表。
     *
     * @param maxSubscriptionsPerConnection 单连接最大订阅数，0 或负数表示不限制
     */
    public SubscriptionRegistry(int maxSubscriptionsPerConnection) {
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;
    }

    /**
     * 注册一个连接对配置键的订阅。
     *
     * @param channel 客户端连接
     * @param key 配置键
     * @return 是否注册成功；超出单连接上限时返回 {@code false}
     */
    public synchronized boolean subscribe(Channel channel, ConfigKey key) {
        Set<ConfigKey> keys = keysByChannel.computeIfAbsent(channel,
                value -> ConcurrentHashMap.newKeySet());
        if (!keys.contains(key) && maxSubscriptionsPerConnection > 0
                && keys.size() >= maxSubscriptionsPerConnection) {
            return false;
        }
        keys.add(key);
        channelsByKey.computeIfAbsent(key, value -> ConcurrentHashMap.newKeySet()).add(channel);
        if (listeningChannels.add(channel)) {
            channel.closeFuture().addListener(future -> remove(channel));
        }
        return true;
    }

    /**
     * 移除一个连接对单个配置键的订阅。
     *
     * @param channel 客户端连接
     * @param key 配置键
     */
    public synchronized void unsubscribe(Channel channel, ConfigKey key) {
        Set<ConfigKey> keys = keysByChannel.get(channel);
        if (keys != null) {
            keys.remove(key);
            if (keys.isEmpty()) {
                keysByChannel.remove(channel, keys);
            }
        }
        Set<Channel> channels = channelsByKey.get(key);
        if (channels == null) {
            return;
        }
        channels.remove(channel);
        if (channels.isEmpty()) {
            channelsByKey.remove(key, channels);
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
    public synchronized void remove(Channel channel) {
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

    /**
     * 返回订阅关系快照，用于管理面观测。
     *
     * <p>按连接列出远端地址与订阅键（键按字典序排序，连接按远端地址排序），
     * 供 {@code GET /v1/cs/subscriptions} 与控制台展示。</p>
     *
     * @return 订阅快照
     */
    public synchronized SubscriptionSnapshot snapshot() {
        List<SubscriptionSnapshot.Connection> connections = new ArrayList<>();
        int total = 0;
        for (Set<ConfigKey> keys : keysByChannel.values()) {
            total += keys.size();
        }
        for (java.util.Map.Entry<Channel, Set<ConfigKey>> entry : keysByChannel.entrySet()) {
            List<String> paths = new ArrayList<>();
            for (ConfigKey key : entry.getValue()) {
                paths.add(key.toPath());
            }
            Collections.sort(paths);
            connections.add(new SubscriptionSnapshot.Connection(
                    formatRemote(entry.getKey().remoteAddress()), paths));
        }
        Collections.sort(connections, java.util.Comparator.comparing(
                SubscriptionSnapshot.Connection::getRemote));
        return new SubscriptionSnapshot(total, connections);
    }

    /**
     * 格式化远端地址为 {@code ip:port}。
     *
     * <p>不用 {@code InetSocketAddress.toString()}：其格式为 {@code 主机名/ip:端口}，
     * 主机名解析不到时会留下一个斜杠开头的残缺展示。</p>
     *
     * @param address 远端地址，可为 {@code null}
     * @return {@code ip:port}；非 inet 地址或为空时退化为 toString / 空串
     */
    private static String formatRemote(java.net.SocketAddress address) {
        if (address instanceof java.net.InetSocketAddress) {
            java.net.InetSocketAddress inet = (java.net.InetSocketAddress) address;
            if (inet.getAddress() != null) {
                return inet.getAddress().getHostAddress() + ":" + inet.getPort();
            }
            return inet.getHostString() + ":" + inet.getPort();
        }
        return address == null ? "" : address.toString();
    }

    /**
     * 返回全部订阅条目总数（key × connection），用于观测与上限校验。
     *
     * @return 订阅总数
     */
    public synchronized int subscriptionCount() {
        int total = 0;
        for (Set<ConfigKey> keys : keysByChannel.values()) {
            total += keys.size();
        }
        return total;
    }
}
