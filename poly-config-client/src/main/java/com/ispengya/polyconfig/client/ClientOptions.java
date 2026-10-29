package com.ispengya.polyconfig.client;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 客户端配置。
 */
public final class ClientOptions {

    /**
     * 服务端地址列表，格式为 host:port。
     */
    private final List<String> servers;

    /**
     * 默认命名空间。
     */
    private final String namespace;

    /**
     * 本地快照目录。
     */
    private final Path snapshotDir;

    /**
     * 请求超时时间。
     */
    private final long requestTimeoutMillis;

    /**
     * 订阅对账周期。
     */
    private final long resyncIntervalMillis;

    private ClientOptions(Builder builder) {
        this.servers = Collections.unmodifiableList(new ArrayList<>(builder.servers));
        this.namespace = builder.namespace;
        this.snapshotDir = builder.snapshotDir;
        this.requestTimeoutMillis = builder.requestTimeoutMillis;
        this.resyncIntervalMillis = builder.resyncIntervalMillis;
    }

    /**
     * 创建配置构造器。
     *
     * @return 构造器
     */
    public static Builder builder() {
        return new Builder();
    }

    public List<String> getServers() {
        return servers;
    }

    public String getNamespace() {
        return namespace;
    }

    public Path getSnapshotDir() {
        return snapshotDir;
    }

    public long getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public long getResyncIntervalMillis() {
        return resyncIntervalMillis;
    }

    /**
     * 客户端配置构造器。
     */
    public static final class Builder {

        /**
         * 服务端地址列表。
         */
        private final List<String> servers = new ArrayList<>();

        /**
         * 默认命名空间。
         */
        private String namespace = "public";

        /**
         * 本地快照目录。
         */
        private Path snapshotDir = Paths.get(System.getProperty("user.home"), ".poly-config");

        /**
         * 请求超时时间。
         */
        private long requestTimeoutMillis = 3000L;

        /**
         * 订阅对账周期。
         */
        private long resyncIntervalMillis = 30000L;

        /**
         * 添加一个服务端地址。
         *
         * @param address host:port
         * @return 构造器
         */
        public Builder server(String address) {
            if (address != null && !address.trim().isEmpty()) {
                this.servers.add(address.trim());
            }
            return this;
        }

        /**
         * 添加多个服务端地址。
         *
         * @param addresses host:port 列表
         * @return 构造器
         */
        public Builder servers(String... addresses) {
            if (addresses != null) {
                for (String address : addresses) {
                    server(address);
                }
            }
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder snapshotDir(Path snapshotDir) {
            this.snapshotDir = snapshotDir;
            return this;
        }

        public Builder requestTimeoutMillis(long requestTimeoutMillis) {
            this.requestTimeoutMillis = requestTimeoutMillis;
            return this;
        }

        public Builder resyncIntervalMillis(long resyncIntervalMillis) {
            this.resyncIntervalMillis = resyncIntervalMillis;
            return this;
        }

        public ClientOptions build() {
            if (servers.isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "至少需要一个服务端地址");
            }
            for (String server : servers) {
                if (!server.contains(":")) {
                    throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                            "服务端地址格式应为 host:port: " + server);
                }
            }
            if (namespace == null || namespace.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "namespace 不能为空");
            }
            if (snapshotDir == null) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "snapshotDir 不能为空");
            }
            if (requestTimeoutMillis <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "requestTimeoutMillis 必须大于 0");
            }
            if (resyncIntervalMillis <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "resyncIntervalMillis 必须大于 0");
            }
            return new ClientOptions(this);
        }
    }
}
