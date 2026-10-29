package com.ispengya.polyconfig.common.model;

import java.util.Objects;

/**
 * 配置历史版本。
 *
 * <p>发布、删除都会写入历史，用于审计和回滚。</p>
 */
public final class HistoryEntry {

    /**
     * 配置键。
     */
    private final ConfigKey key;

    /**
     * 该历史版本对应的变更序号。
     */
    private final long revision;

    /**
     * 变更操作。
     */
    private final ConfigOperation operation;

    /**
     * 变更后的内容；删除操作记录删除前的内容。
     */
    private final String content;

    /**
     * 内容类型。
     */
    private final ConfigType type;

    /**
     * 内容 MD5。
     */
    private final String md5;

    /**
     * 操作人。
     */
    private final String operator;

    /**
     * 变更时间。
     */
    private final long timestamp;

    /**
     * 创建历史条目。
     *
     * @param key 配置键
     * @param revision 变更序号
     * @param operation 变更操作
     * @param content 变更内容
     * @param type 内容类型
     * @param md5 内容 MD5
     * @param operator 操作人
     * @param timestamp 变更时间
     */
    public HistoryEntry(ConfigKey key, long revision, ConfigOperation operation, String content,
                        ConfigType type, String md5, String operator, long timestamp) {
        this.key = Objects.requireNonNull(key, "key");
        this.revision = revision;
        this.operation = Objects.requireNonNull(operation, "operation");
        this.content = content == null ? "" : content;
        this.type = type == null ? ConfigType.TEXT : type;
        this.md5 = md5 == null ? "" : md5;
        this.operator = operator == null ? "" : operator;
        this.timestamp = timestamp;
    }

    public ConfigKey getKey() {
        return key;
    }

    public long getRevision() {
        return revision;
    }

    public ConfigOperation getOperation() {
        return operation;
    }

    public String getContent() {
        return content;
    }

    public ConfigType getType() {
        return type;
    }

    public String getMd5() {
        return md5;
    }

    public String getOperator() {
        return operator;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
