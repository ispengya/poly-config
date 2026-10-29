package com.ispengya.polyconfig.common.error;

/**
 * poly-config 业务异常。
 *
 * <p>保留错误码，便于接入层映射为 HTTP 状态码和统一响应体。</p>
 */
public class PolyConfigException extends RuntimeException {

    /**
     * 业务错误码。
     */
    private final ErrorCode errorCode;

    /**
     * 使用错误码默认信息创建异常。
     *
     * @param errorCode 错误码
     */
    public PolyConfigException(ErrorCode errorCode) {
        this(errorCode, errorCode.getMessage(), null);
    }

    /**
     * 使用自定义信息创建异常。
     *
     * @param errorCode 错误码
     * @param message 错误信息
     */
    public PolyConfigException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    /**
     * 使用自定义信息和原因创建异常。
     *
     * @param errorCode 错误码
     * @param message 错误信息
     * @param cause 原始异常
     */
    public PolyConfigException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
