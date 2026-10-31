package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 服务端启动配置。
 */
public final class ServerOptions {

    /**
     * 监听地址。
     */
    private final String host;

    /**
     * 对外展示的主机名或 IP，为空时自动探测。
     */
    private final String advertiseHost;

    /**
     * HTTP 端口，0 表示随机端口。
     */
    private final int httpPort;

    /**
     * 客户端长连接端口，0 表示随机端口。
     */
    private final int remotingPort;

    /**
     * 数据目录。
     */
    private final Path dataDir;

    /**
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 集群 ID。
     */
    private final String clusterId;

    /**
     * 每个配置键保留的历史条数。
     */
    private final int historyLimit;

    /**
     * 运行模式。
     */
    private final String mode;

    private ServerOptions(Builder builder) {
        this.host = builder.host;
        this.advertiseHost = builder.advertiseHost;
        this.httpPort = builder.httpPort;
        this.remotingPort = builder.remotingPort;
        this.dataDir = builder.dataDir;
        this.nodeId = builder.nodeId;
        this.clusterId = builder.clusterId;
        this.historyLimit = builder.historyLimit;
        this.mode = builder.mode;
    }

    /**
     * 创建配置构造器。
     *
     * @return 构造器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 从命令行参数解析配置。
     *
     * <p>支持 --host、--advertise-host、--http-port、--remoting-port、--data-dir、
     * --node-id、--cluster-id、--history-limit、--mode 九项。</p>
     *
     * @param args 命令行参数
     * @return 启动配置
     */
    public static ServerOptions fromArgs(String[] args) {
        Builder builder = builder();
        if (args == null) {
            return builder.build();
        }
        for (String arg : args) {
            if (arg == null || !arg.startsWith("--")) {
                continue;
            }
            int separator = arg.indexOf('=');
            if (separator < 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "参数格式应为 --key=value: " + arg);
            }
            String key = arg.substring(2, separator);
            String value = arg.substring(separator + 1);
            switch (key) {
                case "host":
                    builder.host(value);
                    break;
                case "advertise-host":
                    builder.advertiseHost(value);
                    break;
                case "http-port":
                    builder.httpPort(Integer.parseInt(value));
                    break;
                case "remoting-port":
                    builder.remotingPort(Integer.parseInt(value));
                    break;
                case "data-dir":
                    builder.dataDir(Paths.get(value));
                    break;
                case "node-id":
                    builder.nodeId(value);
                    break;
                case "cluster-id":
                    builder.clusterId(value);
                    break;
                case "history-limit":
                    builder.historyLimit(Integer.parseInt(value));
                    break;
                case "mode":
                    builder.mode(value);
                    break;
                default:
                    throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                            "未知启动参数: " + key);
            }
        }
        return builder.build();
    }

    public String getHost() {
        return host;
    }

    public String getAdvertiseHost() {
        return advertiseHost;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getRemotingPort() {
        return remotingPort;
    }

    public Path getDataDir() {
        return dataDir;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public int getHistoryLimit() {
        return historyLimit;
    }

    public String getMode() {
        return mode;
    }

    /**
     * 启动配置构造器。
     */
    public static final class Builder {

        /**
         * 监听地址。
         *
         * <p>默认只绑定回环地址；对外提供服务时显式传 {@code 0.0.0.0} 或具体网卡地址，
         * 并配合防火墙/鉴权。</p>
         */
        private String host = "127.0.0.1";

        /**
         * 对外展示的主机名或 IP，为空时自动探测。
         */
        private String advertiseHost;

        /**
         * HTTP 端口。
         */
        private int httpPort = 8848;

        /**
         * 客户端长连接端口。
         */
        private int remotingPort = 9848;

        /**
         * 数据目录。
         */
        private Path dataDir = Paths.get("data");

        /**
         * 节点 ID。
         */
        private String nodeId = "node-1";

        /**
         * 集群 ID。
         */
        private String clusterId = "poly-config";

        /**
         * 每个配置键保留的历史条数。
         */
        private int historyLimit = 100;

        /**
         * 运行模式。
         */
        private String mode = "standalone";

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder advertiseHost(String advertiseHost) {
            this.advertiseHost = advertiseHost;
            return this;
        }

        public Builder httpPort(int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder remotingPort(int remotingPort) {
            this.remotingPort = remotingPort;
            return this;
        }

        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder historyLimit(int historyLimit) {
            this.historyLimit = historyLimit;
            return this;
        }

        public Builder mode(String mode) {
            this.mode = mode;
            return this;
        }

        public ServerOptions build() {
            if (host == null || host.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "host 不能为空");
            }
            if (httpPort < 0 || httpPort > 65535) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "httpPort 必须在 0..65535 之间");
            }
            if (remotingPort < 0 || remotingPort > 65535) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "remotingPort 必须在 0..65535 之间");
            }
            if (dataDir == null) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "dataDir 不能为空");
            }
            if (nodeId == null || nodeId.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "nodeId 不能为空");
            }
            if (clusterId == null || clusterId.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "clusterId 不能为空");
            }
            if (historyLimit <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "historyLimit 必须大于 0");
            }
            if (mode == null || mode.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "mode 不能为空");
            }
            return new ServerOptions(this);
        }
    }
}
