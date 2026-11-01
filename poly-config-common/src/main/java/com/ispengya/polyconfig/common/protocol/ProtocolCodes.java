package com.ispengya.polyconfig.common.protocol;

/**
 * 客户端与服务端之间的协议码。
 */
public final class ProtocolCodes {

    /**
     * 订阅配置变更。
     */
    public static final int SUBSCRIBE = 1001;

    /**
     * 拉取配置内容。
     */
    public static final int FETCH = 1002;

    /**
     * 取消订阅配置变更。
     */
    public static final int UNSUBSCRIBE = 1003;

    /**
     * 批量订阅/对账。
     */
    public static final int BATCH_SUBSCRIBE = 1004;

    /**
     * 服务端推送配置变更。
     */
    public static final int PUSH_CONFIG_CHANGED = 2001;

    /**
     * 响应成功。
     */
    public static final int RESPONSE_OK = 200;

    /**
     * 请求参数错误。
     */
    public static final int RESPONSE_BAD_REQUEST = 400;

    /**
     * 资源不存在。
     */
    public static final int RESPONSE_NOT_FOUND = 404;

    /**
     * 服务端内部错误。
     */
    public static final int RESPONSE_SERVER_ERROR = 500;

    private ProtocolCodes() {
    }
}
