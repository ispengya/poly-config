package com.ispengya.polyconfig.common.util;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

/**
 * 参数校验工具。
 *
 * <p>校验失败统一抛出 {@link PolyConfigException}，错误码为 {@link ErrorCode#INVALID_ARGUMENT}。</p>
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * 校验字符串非空，并返回去除首尾空白后的值。
     *
     * @param value 待校验字符串
     * @param field 字段名，用于错误信息
     * @return 去除首尾空白后的字符串
     */
    public static String checkNotBlank(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, field + " 不能为空");
        }
        return value.trim();
    }

    /**
     * 校验对象非空。
     *
     * @param value 待校验对象
     * @param field 字段名，用于错误信息
     * @param <T> 对象类型
     * @return 原对象
     */
    public static <T> T checkNotNull(T value, String field) {
        if (value == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, field + " 不能为空");
        }
        return value;
    }

    /**
     * 校验条件成立。
     *
     * @param condition 条件
     * @param message 错误信息
     */
    public static void checkArgument(boolean condition, String message) {
        if (!condition) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, message);
        }
    }
}
