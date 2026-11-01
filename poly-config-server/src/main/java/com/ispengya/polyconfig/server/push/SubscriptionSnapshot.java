package com.ispengya.polyconfig.server.push;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 订阅关系快照，用于管理面观测（GET /v1/cs/subscriptions）。
 *
 * <p>只读视图：按连接列出远端地址与订阅键，供控制台展示"谁连着我、订了什么"。
 * 订阅表是节点本地的，每个节点只反映连着自己的客户端。</p>
 */
public final class SubscriptionSnapshot {

    /**
     * 订阅总数（key × connection）。
     */
    private final int total;

    /**
     * 按远端地址排序的连接列表。
     */
    private final List<Connection> connections;

    /**
     * 创建订阅快照。
     *
     * @param total 订阅总数
     * @param connections 连接订阅列表
     */
    public SubscriptionSnapshot(int total, List<Connection> connections) {
        this.total = total;
        this.connections = connections == null
                ? Collections.<Connection>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(connections));
    }

    public int getTotal() {
        return total;
    }

    public List<Connection> getConnections() {
        return connections;
    }

    /**
     * 单个连接的订阅视图。
     */
    public static final class Connection {

        /**
         * 远端地址（host:port）；无法获取时为空串。
         */
        private final String remote;

        /**
         * 订阅键列表（namespace/group/dataId），按字典序排序。
         */
        private final List<String> keys;

        /**
         * 创建连接订阅视图。
         *
         * @param remote 远端地址
         * @param keys 订阅键列表
         */
        public Connection(String remote, List<String> keys) {
            this.remote = remote == null ? "" : remote;
            this.keys = keys == null
                    ? Collections.<String>emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(keys));
        }

        public String getRemote() {
            return remote;
        }

        public int getCount() {
            return keys.size();
        }

        public List<String> getKeys() {
            return keys;
        }
    }
}
