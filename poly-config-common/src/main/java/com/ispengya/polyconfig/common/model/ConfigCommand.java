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
     * 乐观锁期望版本；{@code null} 表示不检查。
     *
     * <p>{@code -1} 表示"配置必须不存在"；其他值必须等于配置当前 revision。
     * 该字段是写入前的前置条件，由协议层在追加日志之前校验，因此不参与
     * {@code CommandCodec} 编码——日志中只存校验通过的命令，回放时不重复校验，
     * 存储格式保持不变。</p>
     */
    private final Long expectedVersion;

    /**
     * 副本路径携带的 owner 分配版本；本地分配时为 {@code null}。
     *
     * <p>AP 模式下，副本（含转发节点同步 apply）应用复制来的命令时使用 owner
     * 在分配时刻的版本号，而不是本地 revision——revision 是节点本地位点，
     * version 才是跨节点权威序号。该字段参与 {@code CommandCodec} 编码，
     * 保证重启回放后版本与 owner 一致；回放命令不携带时按"本地分配"处理。</p>
     */
    private Long assignedVersion;

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
        this(operation, key, content, type, operator, timestamp, null);
    }

    /**
     * 创建带乐观锁期望版本的配置命令。
     *
     * @param operation 变更操作
     * @param key 配置键
     * @param content 配置内容
     * @param type 内容类型
     * @param operator 操作人
     * @param timestamp 命令创建时间
     * @param expectedVersion 期望版本；{@code null} 表示不检查，{@code -1} 表示必须不存在
     */
    public ConfigCommand(ConfigOperation operation, ConfigKey key, String content,
                         ConfigType type, String operator, long timestamp, Long expectedVersion) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.key = Objects.requireNonNull(key, "key");
        this.content = content == null ? "" : content;
        this.type = type == null ? ConfigType.TEXT : type;
        this.operator = operator == null ? "" : operator;
        this.timestamp = timestamp;
        this.expectedVersion = expectedVersion;
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

    public Long getExpectedVersion() {
        return expectedVersion;
    }

    public Long getAssignedVersion() {
        return assignedVersion;
    }

    public void setAssignedVersion(Long assignedVersion) {
        this.assignedVersion = assignedVersion;
    }
}
