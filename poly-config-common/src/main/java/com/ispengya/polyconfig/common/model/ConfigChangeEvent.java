package com.ispengya.polyconfig.common.model;

import java.util.Objects;

/**
 * 配置变更事件。
 *
 * <p>状态机应用命令后产生，通知引擎据此向订阅客户端推送变更元数据。</p>
 */
public final class ConfigChangeEvent {

    /**
     * 配置键。
     */
    private final ConfigKey key;

    /**
     * 变更操作。
     */
    private final ConfigOperation operation;

    /**
     * 变更后的内容 MD5；删除操作为空字符串。
     */
    private final String md5;

    /**
     * 变更后的条目版本。
     */
    private final long version;

    /**
     * 变更序号。
     */
    private final long revision;

    /**
     * 变更时间。
     */
    private final long timestamp;

    /**
     * 创建变更事件。
     *
     * @param key 配置键
     * @param operation 变更操作
     * @param md5 内容 MD5
     * @param version 条目版本
     * @param revision 变更序号
     * @param timestamp 变更时间
     */
    public ConfigChangeEvent(ConfigKey key, ConfigOperation operation, String md5,
                             long version, long revision, long timestamp) {
        this.key = Objects.requireNonNull(key, "key");
        this.operation = Objects.requireNonNull(operation, "operation");
        this.md5 = md5 == null ? "" : md5;
        this.version = version;
        this.revision = revision;
        this.timestamp = timestamp;
    }

    public ConfigKey getKey() {
        return key;
    }

    public ConfigOperation getOperation() {
        return operation;
    }

    public String getMd5() {
        return md5;
    }

    public long getVersion() {
        return version;
    }

    public long getRevision() {
        return revision;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
