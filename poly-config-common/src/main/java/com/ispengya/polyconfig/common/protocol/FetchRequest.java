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

    /**
     * 读一致性级别（{@code LOCAL}/{@code LINEARIZABLE}，D8）；
     * {@code null} 视为 {@code LOCAL}，兼容旧客户端。
     */
    private String consistency;

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

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }
}
