package com.ispengya.polyconfig.common.codec;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;

/**
 * 配置命令的日志记录形态。
 *
 * <p>使用扁平字段便于 JSON 序列化，避免 ConfigKey 的嵌套构造问题。</p>
 */
public class CommandRecord {

    /**
     * 变更操作名称。
     */
    private String operation;

    /**
     * 命名空间。
     */
    private String namespace;

    /**
     * 分组。
     */
    private String group;

    /**
     * 配置项 ID。
     */
    private String dataId;

    /**
     * 配置内容。
     */
    private String content;

    /**
     * 内容类型名称。
     */
    private String type;

    /**
     * 操作人。
     */
    private String operator;

    /**
     * 命令创建时间。
     */
    private long timestamp;

    /**
     * owner 分配的版本号；本地分配的命令为 {@code null}（旧日志同样缺省）。
     *
     * <p>JSON 可选字段：旧记录没有该字段时回放按"本地分配"处理，向后兼容。</p>
     */
    private Long version;

    public CommandRecord() {
    }

    /**
     * 从配置命令创建日志记录。
     *
     * @param command 配置命令
     * @return 日志记录
     */
    public static CommandRecord from(ConfigCommand command) {
        CommandRecord record = new CommandRecord();
        record.operation = command.getOperation().name();
        record.namespace = command.getKey().getNamespace();
        record.group = command.getKey().getGroup();
        record.dataId = command.getKey().getDataId();
        record.content = command.getContent();
        record.type = command.getType().name();
        record.operator = command.getOperator();
        record.timestamp = command.getTimestamp();
        record.version = command.getAssignedVersion();
        return record;
    }

    /**
     * 还原为配置命令。
     *
     * @return 配置命令
     */
    public ConfigCommand toCommand() {
        try {
            ConfigOperation configOperation = ConfigOperation.valueOf(operation);
            ConfigKey key = new ConfigKey(namespace, group, dataId);
            ConfigCommand command = new ConfigCommand(configOperation, key, content,
                    ConfigType.from(type), operator, timestamp);
            command.setAssignedVersion(version);
            return command;
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志命令字段非法", e);
        }
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }
}
