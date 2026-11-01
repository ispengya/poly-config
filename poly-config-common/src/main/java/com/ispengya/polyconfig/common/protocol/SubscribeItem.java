package com.ispengya.polyconfig.common.protocol;

/**
 * 批量订阅中的单个配置键及其客户端侧版本信息。
 *
 * <p>客户端用 {@code md5} 与 {@code revision} 声明"我手上已有的版本"，
 * 服务端据此跳过未变更的键、识别已删除的键。</p>
 */
public class SubscribeItem {

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
     * 客户端当前持有的内容 MD5，首次订阅为空字符串。
     */
    private String md5;

    /**
     * 客户端当前持有的条目 revision，未知时为 0。
     */
    private long revision;

    public SubscribeItem() {
    }

    /**
     * 创建订阅条目。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param md5 客户端当前 MD5
     * @param revision 客户端当前 revision，未知时传 0
     * @return 订阅条目
     */
    public static SubscribeItem of(String namespace, String group, String dataId,
                                   String md5, long revision) {
        SubscribeItem item = new SubscribeItem();
        item.namespace = namespace;
        item.group = group;
        item.dataId = dataId;
        item.md5 = md5;
        item.revision = revision;
        return item;
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

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }
}
