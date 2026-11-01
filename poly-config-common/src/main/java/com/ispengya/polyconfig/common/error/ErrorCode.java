package com.ispengya.polyconfig.common.error;

/**
 * 统一错误码。
 *
 * <p>业务异常通过 {@link PolyConfigException} 携带错误码向上抛出，接入层负责映射为 HTTP 状态码。</p>
 */
public enum ErrorCode {

    INVALID_ARGUMENT(40001, "参数不合法"),
    PAYLOAD_TOO_LARGE(41301, "请求体过大"),
    NOT_FOUND(40401, "配置不存在"),
    VERSION_CONFLICT(40901, "配置版本冲突"),
    STORAGE_ERROR(50001, "存储错误"),
    NOT_LEADER(50301, "当前节点不是 leader"),
    UNAVAILABLE(50302, "服务不可用"),
    INTERNAL(50000, "内部错误");

    /**
     * 错误码数值。
     */
    private final int code;

    /**
     * 默认错误信息。
     */
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
