package com.ispengya.polyconfig.common.protocol;

/**
 * 周期校验摘要条目：{@code key → version + 是否墓碑}。
 *
 * <p>不含配置内容，全量摘要表保持轻量；增量内容由校验应答携带。</p>
 */
public class VerifyEntry {

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
     * 条目版本（活条目为跨节点权威 version；墓碑为删除时的 owner 版本）。
     */
    private long version;

    /**
     * 是否墓碑（该键已被删除）。
     */
    private boolean deleted;

    public VerifyEntry() {
    }

    /**
     * 创建摘要条目。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param version 条目版本
     * @param deleted 是否墓碑
     */
    public VerifyEntry(String namespace, String group, String dataId, long version,
                       boolean deleted) {
        this.namespace = namespace;
        this.group = group;
        this.dataId = dataId;
        this.version = version;
        this.deleted = deleted;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
