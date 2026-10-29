package com.ispengya.polyconfig.common.protocol;

/**
 * 客户端拉取配置请求。
 */
public class FetchRequest {

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

    public FetchRequest() {
    }

    /**
     * 创建拉取请求。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @return 拉取请求
     */
    public static FetchRequest of(String namespace, String group, String dataId) {
        FetchRequest request = new FetchRequest();
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
