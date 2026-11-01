package com.ispengya.polyconfig.common.protocol;

/**
 * AP 线性读请求（任意节点 → owner）。
 */
public class ForwardReadRequest {

    private String namespace;
    private String group;
    private String dataId;

    public ForwardReadRequest() {
    }

    /**
     * 创建线性读请求。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @return 线性读请求
     */
    public static ForwardReadRequest of(String namespace, String group, String dataId) {
        ForwardReadRequest request = new ForwardReadRequest();
        request.namespace = namespace;
        request.group = group;
        request.dataId = dataId;
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
}
