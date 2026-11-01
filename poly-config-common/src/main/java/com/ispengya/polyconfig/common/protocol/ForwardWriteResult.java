package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.MutationResult;

/**
 * AP 写转发应答（owner → 转发节点）。
 *
 * <p>携带 owner 分配的 version 与完整命令回显，转发节点据此同步 apply
 * （写后即读）并落入本地日志。</p>
 */
public class ForwardWriteResult {

    private String operation;
    private String namespace;
    private String group;
    private String dataId;
    private String content;
    private String type;
    private String operator;
    private long timestamp;
    private long version;
    private long revision;
    private String md5;

    public ForwardWriteResult() {
    }

    /**
     * 从 owner 的提交结果创建转发应答。
     *
     * @param command 已应用的命令
     * @param result 提交结果
     * @return 转发应答
     */
    public static ForwardWriteResult from(ConfigCommand command, MutationResult result) {
        ForwardWriteResult forward = new ForwardWriteResult();
        forward.operation = command.getOperation().name();
        forward.namespace = command.getKey().getNamespace();
        forward.group = command.getKey().getGroup();
        forward.dataId = command.getKey().getDataId();
        forward.content = command.getContent();
        forward.type = command.getType().name();
        forward.operator = command.getOperator();
        forward.timestamp = command.getTimestamp();
        forward.version = result.getVersion();
        forward.revision = result.getRevision();
        forward.md5 = result.getMd5();
        return forward;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }
}
