package com.ispengya.polyconfig.common.protocol;

/**
 * HTTP API 统一响应体。
 *
 * @param <T> 数据类型
 */
public class ApiResponse<T> {

    /**
     * 业务码，0 表示成功。
     */
    private int code;

    /**
     * 提示信息。
     */
    private String message;

    /**
     * 响应数据。
     */
    private T data;

    public ApiResponse() {
    }

    /**
     * 创建成功响应。
     *
     * @param data 响应数据
     * @param <T> 数据类型
     * @return 响应体
     */
    public static <T> ApiResponse<T> ok(T data) {
        ApiResponse<T> response = new ApiResponse<>();
        response.code = 0;
        response.message = "ok";
        response.data = data;
        return response;
    }

    /**
     * 创建错误响应。
     *
     * @param code 业务码
     * @param message 错误信息
     * @param <T> 数据类型
     * @return 响应体
     */
    public static <T> ApiResponse<T> error(int code, String message) {
        ApiResponse<T> response = new ApiResponse<>();
        response.code = code;
        response.message = message;
        return response;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
