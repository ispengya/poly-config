package com.ispengya.polyconfig.common.protocol;

/**
 * 客户端订阅请求。
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

    public SubscribeRequest() {
    }

    /**
     * 创建订阅请求。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param md5 客户端当前 MD5
     * @return 订阅请求
     */
    public static SubscribeRequest of(String namespace, String group, String dataId, String md5) {
        SubscribeRequest request = new SubscribeRequest();
        request.namespace = namespace;
        request.group = group;
        request.dataId = dataId;
        request.md5 = md5;
        return request;
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
