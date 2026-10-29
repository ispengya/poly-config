package com.ispengya.polyconfig.client;

/**
 * 客户端配置服务。
 */
public interface ConfigService extends AutoCloseable {

    /**
     * 读取配置。
     *
     * @param namespace 命名空间，null 时使用客户端默认值
     * @param group 分组
     * @param dataId 配置项 ID
     * @param timeoutMillis 请求超时时间
     * @return 配置内容；不存在时返回 null
     */
    String getConfig(String namespace, String group, String dataId, long timeoutMillis);

    /**
     * 使用默认命名空间读取配置。
     *
     * @param group 分组
     * @param dataId 配置项 ID
     * @param timeoutMillis 请求超时时间
     * @return 配置内容；不存在时返回 null
     */
    default String getConfig(String group, String dataId, long timeoutMillis) {
        return getConfig(null, group, dataId, timeoutMillis);
    }

    /**
     * 注册配置监听器。
     *
     * @param namespace 命名空间，null 时使用客户端默认值
     * @param group 分组
     * @param dataId 配置项 ID
     * @param listener 监听器
     */
    void addListener(String namespace, String group, String dataId, ConfigListener listener);

    /**
     * 使用默认命名空间注册监听器。
     *
     * @param group 分组
     * @param dataId 配置项 ID
     * @param listener 监听器
     */
    default void addListener(String group, String dataId, ConfigListener listener) {
        addListener(null, group, dataId, listener);
    }

    /**
     * 移除配置监听器。
     *
     * @param namespace 命名空间，null 时使用客户端默认值
     * @param group 分组
     * @param dataId 配置项 ID
     * @param listener 监听器
     */
    void removeListener(String namespace, String group, String dataId, ConfigListener listener);

    /**
     * 使用默认命名空间移除监听器。
     *
     * @param group 分组
     * @param dataId 配置项 ID
     * @param listener 监听器
     */
    default void removeListener(String group, String dataId, ConfigListener listener) {
        removeListener(null, group, dataId, listener);
    }

    @Override
    void close();
}
