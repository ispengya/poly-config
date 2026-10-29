package com.ispengya.polyconfig.common.protocol;

/**
 * 创建分组请求。
 */
public class CreateGroupRequest {

    /**
     * 命名空间名称。
     */
    private String namespace;

    /**
     * 分组名称。
     */
    private String group;

    public CreateGroupRequest() {
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
}
