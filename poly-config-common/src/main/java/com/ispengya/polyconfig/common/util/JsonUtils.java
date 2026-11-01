package com.ispengya.polyconfig.common.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.TypeReference;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.util.List;

/**
 * JSON 序列化工具。
 *
 * <p>统一封装 fastjson2，并把解析异常转换为业务异常。</p>
 */
public final class JsonUtils {

    private JsonUtils() {
    }

    /**
     * 将对象序列化为 JSON 字符串。
     *
     * @param value 待序列化对象
     * @return JSON 字符串
     */
    public static String toJson(Object value) {
        return JSON.toJSONString(value);
    }

    /**
     * 将对象序列化为格式化（缩进换行）的 JSON 字符串。
     *
     * <p>仅用于落盘的元数据/快照文件，便于人工排查；解析端对空白不敏感，
     * 与紧凑格式互相兼容。WAL 记录与网络报文仍使用紧凑的 {@link #toJson}。</p>
     *
     * @param value 待序列化对象
     * @return 带缩进换行的 JSON 字符串
     */
    public static String toPrettyJson(Object value) {
        return JSON.toJSONString(value, JSONWriter.Feature.PrettyFormat);
    }

    /**
     * 将 JSON 字符串解析为对象。
     *
     * @param json JSON 字符串
     * @param type 目标类型
     * @param <T> 目标类型
     * @return 解析结果
     */
    public static <T> T parseObject(String json, Class<T> type) {
        try {
            return JSON.parseObject(json, type);
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "JSON 解析失败", e);
        }
    }

    /**
     * 将 JSON 字符串解析为泛型对象。
     *
     * @param json JSON 字符串
     * @param type 目标类型引用
     * @param <T> 目标类型
     * @return 解析结果
     */
    public static <T> T parseObject(String json, TypeReference<T> type) {
        try {
            return JSON.parseObject(json, type);
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "JSON 解析失败", e);
        }
    }

    /**
     * 将 JSON 数组解析为列表。
     *
     * @param json JSON 字符串
     * @param type 元素类型
     * @param <T> 元素类型
     * @return 解析结果
     */
    public static <T> List<T> parseArray(String json, Class<T> type) {
        try {
            return JSON.parseArray(json, type);
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "JSON 解析失败", e);
        }
    }
}
