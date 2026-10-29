package com.ispengya.polyconfig.client;

/**
 * 客户端配置监听器。
 */
public interface ConfigListener {

    /**
     * 配置变更回调。
     *
     * @param dataId 配置项 ID
     * @param group 分组
     * @param content 最新内容；配置被删除时为 null
     */
    void onChange(String dataId, String group, String content);
}
