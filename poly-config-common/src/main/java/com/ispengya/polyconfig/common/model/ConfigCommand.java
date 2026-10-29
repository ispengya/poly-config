package com.ispengya.polyconfig.common.model;

import java.util.Objects;

/**
 * 配置变更命令。
 *
 * <p>命令是可复制、可回放的，状态机只依赖命令本身，不依赖调用方的上下文。</p>
 */
public final class ConfigCommand {

    /**
     * 变更操作。
     */
    private final ConfigOperation operation;

    /**
     * 配置键。
     */
    private final ConfigKey key;

    /**
     * 变更内容；删除命令为空字符串。
     */
    private final String content;

    /**
     * 内容类型。
     */
    private final ConfigType type;

    /**
     * 操作人。
     */
    private final String operator;

    /**
     * 命令创建时间。
     */
    private final long timestamp;

    /**
     * 创建配置命令。
     *
     * @param operation 变更操作
     * @param key 配置键
     * @param content 配置内容
     * @param type 内容类型
     * @param operator 操作人
     * @param timestamp 命令创建时间
     */
    public ConfigCommand(ConfigOperation operation, ConfigKey key, String content,
                         ConfigType type, String operator, long timestamp) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.key = Objects.requireNonNull(key, "key");
        this.content = content == null ? "" : content;
        this.type = type == null ? ConfigType.TEXT : type;
        this.operator = operator == null ? "" : operator;
        this.timestamp = timestamp;
    }

    /**
     * 创建发布命令。
     *
     * @param key 配置键
     * @param content 配置内容
     * @param type 内容类型
     * @param operator 操作人
     * @return 发布命令
     */
    public static ConfigCommand publish(ConfigKey key, String content, ConfigType type, String operator) {
        return new ConfigCommand(ConfigOperation.PUBLISH, key, content, type, operator,
                System.currentTimeMillis());
    }

    /**
     * 创建删除命令。
     *
     * @param key 配置键
     * @param operator 操作人
     * @return 删除命令
     */
    public static ConfigCommand delete(ConfigKey key, String operator) {
        return new ConfigCommand(ConfigOperation.DELETE, key, "", ConfigType.TEXT, operator,
                System.currentTimeMillis());
    }

    /**
     * 创建回滚命令。
     *
     * @param key 配置键
     * @param content 回滚后的内容
     * @param type 内容类型
     * @param operator 操作人
     * @return 回滚命令
     */
    public static ConfigCommand rollback(ConfigKey key, String content, ConfigType type, String operator) {
        return new ConfigCommand(ConfigOperation.ROLLBACK, key, content, type, operator,
                System.currentTimeMillis());
    }

    public ConfigOperation getOperation() {
        return operation;
    }

    public ConfigKey getKey() {
        return key;
    }

    public String getContent() {
        return content;
    }

    public ConfigType getType() {
        return type;
    }

    public String getOperator() {
        return operator;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
