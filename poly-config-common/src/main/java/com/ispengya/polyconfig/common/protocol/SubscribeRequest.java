package com.ispengya.polyconfig.common.protocol;

/**
 * 客户端订阅请求。
 *
 * <p>兼容性：字段 {@code revision} 更名为 {@code version}（跨节点权威序号）。
 * 新客户端对旧服务端：旧端读不到 revision 字段按 0 处理，md5 兜底仍然生效；
 * 旧客户端对新服务端同理。</p>
 */
public class SubscribeRequest {

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
     * 客户端当前持有的条目 version（跨节点权威），首次订阅或未知时为 0。
     */
    private long version;

    public SubscribeRequest() {
    }

    /**
     * 创建订阅请求。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param md5 客户端当前 MD5
     * @param version 客户端当前 version，未知时传 0
     * @return 订阅请求
     */
    public static SubscribeRequest of(String namespace, String group, String dataId,
                                      String md5, long version) {
        SubscribeRequest request = new SubscribeRequest();
        request.namespace = namespace;
        request.group = group;
        request.dataId = dataId;
        request.md5 = md5;
        request.version = version;
        return request;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
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
}
