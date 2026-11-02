package com.ispengya.polyconfig.common.model;

/**
 * 配置中心通用常量。
 */
public final class ConfigConstants {

    /**
     * 默认命名空间。
     */
    public static final String DEFAULT_NAMESPACE = "public";

    /**
     * 默认分组。
     */
    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    /**
     * 内置系统命名空间（AP 模式 workspace 元数据的归属，不对外展示）。
     */
    public static final String SYSTEM_NAMESPACE = "_system";

    /**
     * 内置工作区标记分组（AP 模式 ns/group 标记条目的归属，不对外展示）。
     */
    public static final String WORKSPACE_GROUP = "_workspaces";

    private ConfigConstants() {
    }
}
