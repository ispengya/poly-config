package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.model.HistoryEntry;

/**
 * 配置历史视图，用于 HTTP 响应。
 */
public class HistoryView {

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
     * 变更序号。
     */
    private long revision;

    /**
     * 变更操作。
     */
    private String operation;

    /**
     * 变更内容。
     */
    private String content;

    /**
     * 内容类型。
     */
    private String type;

    /**
     * 内容 MD5。
     */
    private String md5;

    /**
     * 操作人。
     */
    private String operator;

    /**
     * 变更时间。
     */
    private long timestamp;

    public HistoryView() {
    }

    /**
     * 从历史条目创建视图。
     *
     * @param entry 历史条目
     * @return 历史视图
     */
    public static HistoryView from(HistoryEntry entry) {
        HistoryView view = new HistoryView();
        view.namespace = entry.getKey().getNamespace();
        view.group = entry.getKey().getGroup();
        view.dataId = entry.getKey().getDataId();
        view.revision = entry.getRevision();
        view.operation = entry.getOperation().name();
        view.content = entry.getContent();
        view.type = entry.getType().name();
        view.md5 = entry.getMd5();
        view.operator = entry.getOperator();
        view.timestamp = entry.getTimestamp();
        return view;
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

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
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

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
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
}
