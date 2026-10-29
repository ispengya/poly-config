package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.model.ConfigEntry;

/**
 * 配置视图，用于 HTTP 响应和客户端交互。
 */
public class ConfigView {

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
     * 内容类型。
     */
    private String type;

    /**
     * 内容 MD5。
     */
    private String md5;

    /**
     * 条目版本。
     */
    private long version;

    /**
     * 变更序号。
     */
    private long revision;

    /**
     * 创建时间。
     */
    private long createdTime;

    /**
     * 最后更新时间。
     */
    private long modifiedTime;

    /**
     * 最后操作人。
     */
    private String operator;

    public ConfigView() {
    }

    /**
     * 从配置条目创建视图。
     *
     * @param entry 配置条目
     * @return 配置视图
     */
    public static ConfigView from(ConfigEntry entry) {
        if (entry == null) {
            return null;
        }
        ConfigView view = new ConfigView();
        view.namespace = entry.getKey().getNamespace();
        view.group = entry.getKey().getGroup();
        view.dataId = entry.getKey().getDataId();
        view.content = entry.getContent();
        view.type = entry.getType().name();
        view.md5 = entry.getMd5();
        view.version = entry.getVersion();
        view.revision = entry.getRevision();
        view.createdTime = entry.getCreatedTime();
        view.modifiedTime = entry.getModifiedTime();
        view.operator = entry.getOperator();
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

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
