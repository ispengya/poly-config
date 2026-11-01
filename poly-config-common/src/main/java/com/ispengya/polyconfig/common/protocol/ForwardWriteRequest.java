package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;

/**
 * AP 写转发请求（任意节点 → owner）。
 *
 * <p>扁平字段，避免嵌套 {@code ConfigKey} 的 JSON 构造问题（与
 * {@code CommandRecord} 同理）。3xxx 段内部协议，不与客户端协议混用。</p>
 */
public class ForwardWriteRequest {

    private String operation;
    private String namespace;
    private String group;
    private String dataId;
    private String content;
    private String type;
    private String operator;
    private long timestamp;
    private Long expectedVersion;

    public ForwardWriteRequest() {
    }

    /**
     * 从配置命令创建转发请求。
     *
     * @param command 配置命令
     * @return 转发请求
     */
    public static ForwardWriteRequest from(ConfigCommand command) {
        ForwardWriteRequest request = new ForwardWriteRequest();
        request.operation = command.getOperation().name();
        request.namespace = command.getKey().getNamespace();
        request.group = command.getKey().getGroup();
        request.dataId = command.getKey().getDataId();
        request.content = command.getContent();
        request.type = command.getType().name();
        request.operator = command.getOperator();
        request.timestamp = command.getTimestamp();
        request.expectedVersion = command.getExpectedVersion();
        return request;
    }

    /**
     * 还原为配置命令。
     *
     * @return 配置命令（携带 expectedVersion）
     */
    public ConfigCommand toCommand() {
        try {
            return new ConfigCommand(ConfigOperation.valueOf(operation),
                    new ConfigKey(namespace, group, dataId), content,
                    ConfigType.from(type), operator, timestamp, expectedVersion);
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "转发请求字段非法", e);
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

    public Long getExpectedVersion() {
        return expectedVersion;
    }

    public void setExpectedVersion(Long expectedVersion) {
        this.expectedVersion = expectedVersion;
    }
}
