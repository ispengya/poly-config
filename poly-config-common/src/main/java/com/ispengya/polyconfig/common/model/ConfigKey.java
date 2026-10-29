package com.ispengya.polyconfig.common.model;

import com.ispengya.polyconfig.common.util.Preconditions;

import java.util.Objects;

/**
 * 配置唯一键：命名空间 + 分组 + 配置 ID。
 *
 * <p>不可变对象，可直接作为 Map 的 key 使用。</p>
 */
public final class ConfigKey implements Comparable<ConfigKey> {

    /**
     * 命名空间，用于环境或租户隔离。
     */
    private final String namespace;

    /**
     * 分组，通常对应应用名。
     */
    private final String group;

    /**
     * 配置项 ID。
     */
    private final String dataId;

    /**
     * 创建配置键。
     *
     * @param namespace 命名空间，为空时使用默认值
     * @param group 分组，为空时使用默认值
     * @param dataId 配置项 ID，不能为空
     */
    public ConfigKey(String namespace, String group, String dataId) {
        this.namespace = normalize(namespace, ConfigConstants.DEFAULT_NAMESPACE);
        this.group = normalize(group, ConfigConstants.DEFAULT_GROUP);
        this.dataId = Preconditions.checkNotBlank(dataId, "dataId");
    }

    private static String normalize(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    public String getNamespace() {
        return namespace;
    }

    public String getGroup() {
        return group;
    }

    public String getDataId() {
        return dataId;
    }

    /**
     * 返回用于日志和存储的路径形式。
     *
     * @return namespace/group/dataId
     */
    public String toPath() {
        return namespace + "/" + group + "/" + dataId;
    }

    @Override
    public int compareTo(ConfigKey other) {
        return toPath().compareTo(other.toPath());
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        }
        if (!(value instanceof ConfigKey)) {
            return false;
        }
        ConfigKey other = (ConfigKey) value;
        return namespace.equals(other.namespace)
                && group.equals(other.group)
                && dataId.equals(other.dataId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, group, dataId);
    }

    @Override
    public String toString() {
        return toPath();
    }
}
