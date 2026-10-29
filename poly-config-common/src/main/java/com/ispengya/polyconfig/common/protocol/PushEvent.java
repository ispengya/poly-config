package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.model.ConfigChangeEvent;

/**
 * 服务端推送给客户端的配置变更事件。
 */
public class PushEvent {

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
     * 变更操作。
     */
    private String operation;

    /**
     * 变更后的内容 MD5；删除操作为空字符串。
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
     * 变更时间。
     */
    private long timestamp;

    public PushEvent() {
    }

    /**
     * 从变更事件创建推送消息。
     *
     * @param event 变更事件
     * @return 推送消息
     */
    public static PushEvent from(ConfigChangeEvent event) {
        PushEvent push = new PushEvent();
        push.namespace = event.getKey().getNamespace();
        push.group = event.getKey().getGroup();
        push.dataId = event.getKey().getDataId();
        push.operation = event.getOperation().name();
        push.md5 = event.getMd5();
        push.version = event.getVersion();
        push.revision = event.getRevision();
        push.timestamp = event.getTimestamp();
        return push;
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

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
