package com.ispengya.polyconfig.common.protocol;

/**
 * 创建命名空间请求。
 */
public class CreateNamespaceRequest {

    /**
     * 命名空间名称。
     */
    private String namespace;

    public CreateNamespaceRequest() {
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
