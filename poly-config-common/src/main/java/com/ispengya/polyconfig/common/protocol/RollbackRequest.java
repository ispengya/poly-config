package com.ispengya.polyconfig.common.protocol;

/**
 * 回滚配置请求。
 */
public class RollbackRequest {

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
     * 目标历史版本序号。
     */
    private long revision;

    /**
     * 操作人。
     */
    private String operator;

    public RollbackRequest() {
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

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
