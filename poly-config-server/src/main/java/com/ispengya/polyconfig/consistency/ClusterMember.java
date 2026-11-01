package com.ispengya.polyconfig.consistency;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 集群静态成员。
 *
 * <p>成员列表写在各节点配置文件（{@code --cluster-members}），格式为
 * {@code nodeId=host:httpPort:remotingPort}，多个成员用逗号分隔；
 * 变更成员需要修改配置并滚动重启（动态成员为后续里程碑）。</p>
 */
public final class ClusterMember {

    /**
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 节点对外主机名或 IP。
     */
    private final String host;

    /**
     * HTTP 端口。
     */
    private final int httpPort;

    /**
     * 客户端长连接端口。
     */
    private final int remotingPort;

    /**
     * 创建集群成员。
     *
     * @param nodeId 节点 ID
     * @param host 节点对外主机名或 IP
     * @param httpPort HTTP 端口
     * @param remotingPort 客户端长连接端口
     */
    public ClusterMember(String nodeId, String host, int httpPort, int remotingPort) {
        this.nodeId = requireText(nodeId, "nodeId");
        this.host = requireText(host, "host");
        if (httpPort < 1 || httpPort > 65535) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "成员 httpPort 必须在 1..65535 之间: " + httpPort);
        }
        if (remotingPort < 1 || remotingPort > 65535) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "成员 remotingPort 必须在 1..65535 之间: " + remotingPort);
        }
        this.httpPort = httpPort;
        this.remotingPort = remotingPort;
    }

    /**
     * 解析成员列表字符串。
     *
     * <p>格式：{@code nodeId=host:httpPort:remotingPort}，逗号分隔多个成员。
     * 节点 ID 不可重复；host 允许域名或 IP，不含冒号（IPv6 请使用域名或映射地址）。</p>
     *
     * @param value 成员列表字符串，空串返回空列表
     * @return 成员列表（保持书写顺序）
     */
    public static List<ClusterMember> parseList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<ClusterMember> members = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String raw : value.split(",", -1)) {
            String entry = raw.trim();
            if (entry.isEmpty()) {
                throw invalid("成员列表存在空项: " + value);
            }
            ClusterMember member = parseEntry(entry);
            if (!seen.add(member.getNodeId())) {
                throw invalid("成员节点 ID 重复: " + member.getNodeId());
            }
            members.add(member);
        }
        return members;
    }

    private static ClusterMember parseEntry(String entry) {
        int eq = entry.indexOf('=');
        if (eq <= 0 || eq == entry.length() - 1) {
            throw invalid("成员项格式应为 nodeId=host:httpPort:remotingPort: " + entry);
        }
        String nodeId = entry.substring(0, eq).trim();
        String address = entry.substring(eq + 1).trim();
        int lastColon = address.lastIndexOf(':');
        if (lastColon <= 0 || lastColon == address.length() - 1) {
            throw invalid("成员项格式应为 nodeId=host:httpPort:remotingPort: " + entry);
        }
        int remotingPort = parsePort(address.substring(lastColon + 1), entry);
        String head = address.substring(0, lastColon);
        int secondLastColon = head.lastIndexOf(':');
        if (secondLastColon <= 0 || secondLastColon == head.length() - 1) {
            throw invalid("成员项格式应为 nodeId=host:httpPort:remotingPort: " + entry);
        }
        int httpPort = parsePort(head.substring(secondLastColon + 1), entry);
        String host = head.substring(0, secondLastColon).trim();
        if (host.isEmpty()) {
            throw invalid("成员项缺少 host: " + entry);
        }
        return new ClusterMember(nodeId, host, httpPort, remotingPort);
    }

    private static int parsePort(String value, String entry) {
        int port;
        try {
            port = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "成员端口应为整数: " + entry, e);
        }
        if (port < 1 || port > 65535) {
            throw invalid("成员端口必须在 1..65535 之间: " + entry);
        }
        return port;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, name + " 不能为空");
        }
        return value.trim();
    }

    private static PolyConfigException invalid(String reason) {
        return new PolyConfigException(ErrorCode.INVALID_ARGUMENT, reason);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getRemotingPort() {
        return remotingPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterMember)) {
            return false;
        }
        ClusterMember that = (ClusterMember) o;
        return httpPort == that.httpPort
                && remotingPort == that.remotingPort
                && nodeId.equals(that.nodeId)
                && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, host, httpPort, remotingPort);
    }

    @Override
    public String toString() {
        return nodeId + "=" + host + ":" + httpPort + ":" + remotingPort;
    }
}
