package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.consistency.ClusterMember;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;

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
     * 快照触发阈值（命令条数），0 表示关闭快照。
     */
    private final int snapshotThreshold;

    /**
     * 单连接最大订阅数，0 表示不限制。
     */
    private final int maxSubscriptionsPerConnection;

    /**
     * HTTP 请求体最大字节数。
     */
    private final long maxBodyBytes;

    /**
     * 运行模式。
     */
    private final String mode;

    /**
     * 静态集群成员列表；standalone 模式为空。
     */
    private final List<ClusterMember> clusterMembers;

    private ServerOptions(Builder builder) {
        this.host = builder.host;
        this.advertiseHost = builder.advertiseHost;
        this.httpPort = builder.httpPort;
        this.remotingPort = builder.remotingPort;
        this.dataDir = builder.dataDir;
        this.nodeId = builder.nodeId;
        this.clusterId = builder.clusterId;
        this.historyLimit = builder.historyLimit;
        this.snapshotThreshold = builder.snapshotThreshold;
        this.maxSubscriptionsPerConnection = builder.maxSubscriptionsPerConnection;
        this.maxBodyBytes = builder.maxBodyBytes;
        this.mode = builder.mode;
        this.clusterMembers = ClusterMember.parseList(builder.clusterMembers);
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
     * --node-id、--cluster-id、--history-limit、--snapshot-threshold、
     * --max-subscriptions-per-connection、--max-body-bytes、--mode、
     * --cluster-members。</p>
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
                    builder.httpPort(parseIntArg(key, value));
                    break;
                case "remoting-port":
                    builder.remotingPort(parseIntArg(key, value));
                    break;
                case "data-dir":
                    builder.dataDir(parsePathArg(key, value));
                    break;
                case "node-id":
                    builder.nodeId(value);
                    break;
                case "cluster-id":
                    builder.clusterId(value);
                    break;
                case "history-limit":
                    builder.historyLimit(parseIntArg(key, value));
                    break;
                case "snapshot-threshold":
                    builder.snapshotThreshold(parseIntArg(key, value));
                    break;
                case "max-subscriptions-per-connection":
                    builder.maxSubscriptionsPerConnection(parseIntArg(key, value));
                    break;
                case "max-body-bytes":
                    builder.maxBodyBytes(parseLongArg(key, value));
                    break;
                case "mode":
                    builder.mode(value);
                    break;
                case "cluster-members":
                    builder.clusterMembers(value);
                    break;
                default:
                    throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                            "未知启动参数: " + key);
            }
        }
        return builder.build();
    }

    /**
     * 解析整数参数，失败时提示参数名与期望格式。
     *
     * @param key 参数名（不含 --）
     * @param value 参数值
     * @return 整数值
     */
    private static int parseIntArg(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "参数 --" + key + " 的值应为整数: " + value, e);
        }
    }

    /**
     * 解析长整型参数，失败时提示参数名与期望格式。
     *
     * @param key 参数名（不含 --）
     * @param value 参数值
     * @return 长整数值
     */
    private static long parseLongArg(String key, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "参数 --" + key + " 的值应为整数: " + value, e);
        }
    }

    /**
     * 解析路径参数，失败时提示参数名与非法值。
     *
     * @param key 参数名（不含 --）
     * @param value 参数值
     * @return 路径
     */
    private static Path parsePathArg(String key, String value) {
        try {
            return Paths.get(value);
        } catch (InvalidPathException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "参数 --" + key + " 不是合法路径: " + value, e);
        }
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

    public int getSnapshotThreshold() {
        return snapshotThreshold;
    }

    public int getMaxSubscriptionsPerConnection() {
        return maxSubscriptionsPerConnection;
    }

    public long getMaxBodyBytes() {
        return maxBodyBytes;
    }

    public String getMode() {
        return mode;
    }

    /**
     * 返回静态集群成员列表；standalone 模式为空列表。
     *
     * @return 静态集群成员列表
     */
    public List<ClusterMember> getClusterMembers() {
        return clusterMembers;
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
         * 快照触发阈值（命令条数），0 表示关闭快照。
         */
        private int snapshotThreshold = 10000;

        /**
         * 单连接最大订阅数，0 表示不限制。
         */
        private int maxSubscriptionsPerConnection = 1024;

        /**
         * HTTP 请求体最大字节数，默认 1MB。
         */
        private long maxBodyBytes = 1024 * 1024L;

        /**
         * 运行模式，可选 standalone / cp / ap。
         */
        private String mode = "standalone";

        /**
         * 静态集群成员列表原始串，格式 nodeId=host:httpPort:remotingPort，逗号分隔。
         */
        private String clusterMembers = "";

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

        public Builder snapshotThreshold(int snapshotThreshold) {
            this.snapshotThreshold = snapshotThreshold;
            return this;
        }

        public Builder maxSubscriptionsPerConnection(int maxSubscriptionsPerConnection) {
            this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;
            return this;
        }

        public Builder maxBodyBytes(long maxBodyBytes) {
            this.maxBodyBytes = maxBodyBytes;
            return this;
        }

        public Builder mode(String mode) {
            this.mode = mode;
            return this;
        }

        public Builder clusterMembers(String clusterMembers) {
            this.clusterMembers = clusterMembers;
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
            if (snapshotThreshold < 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "snapshotThreshold 不能为负数");
            }
            if (maxSubscriptionsPerConnection < 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "maxSubscriptionsPerConnection 不能为负数");
            }
            if (maxBodyBytes <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "maxBodyBytes 必须大于 0");
            }
            if (mode == null || mode.trim().isEmpty()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "mode 不能为空");
            }
            String normalizedMode = mode.trim().toLowerCase(Locale.ROOT);
            if (!"standalone".equals(normalizedMode)
                    && !"cp".equals(normalizedMode)
                    && !"ap".equals(normalizedMode)) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "未知运行模式: " + mode + "（可选 standalone / cp / ap）");
            }
            List<ClusterMember> members = ClusterMember.parseList(clusterMembers);
            if ("ap".equals(normalizedMode)) {
                if (members.isEmpty()) {
                    throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                            "AP 模式必须通过 --cluster-members 提供静态成员列表，"
                                    + "格式 nodeId=host:httpPort:remotingPort，逗号分隔");
                }
                boolean selfIncluded = false;
                for (ClusterMember member : members) {
                    if (member.getNodeId().equals(nodeId)) {
                        selfIncluded = true;
                        break;
                    }
                }
                if (!selfIncluded) {
                    throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                            "AP 成员列表必须包含本节点 " + nodeId
                                    + "=host:httpPort:remotingPort");
                }
            }
            return new ServerOptions(this);
        }
    }
}
