package com.ispengya.polyconfig.common.model;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 配置内容类型。
 *
 * <p>当前产品只支持 YAML、JSON、PROPERTIES 三种格式；其余枚举值保留用于
 * 兼容旧日志，不再允许通过发布接口写入。</p>
 */
public enum ConfigType {

    TEXT,
    JSON,
    YAML,
    PROPERTIES,
    XML,
    HTML;

    /**
     * 当前产品支持的配置格式。
     */
    public static final List<ConfigType> SUPPORTED = Collections.unmodifiableList(
            Arrays.asList(YAML, JSON, PROPERTIES));

    /**
     * 解析配置类型，无法识别时回退为 {@link #TEXT}。
     *
     * <p>用于读取旧日志，不用于新的发布请求。</p>
     *
     * @param value 类型名称
     * @return 配置类型
     */
    public static ConfigType from(String value) {
        if (value == null || value.trim().isEmpty()) {
            return TEXT;
        }
        String normalized = value.trim();
        for (ConfigType type : values()) {
            if (type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        return TEXT;
    }

    /**
     * 解析发布接口支持的配置类型。
     *
     * <p>空值默认 YAML；只接受 YAML、JSON、PROPERTIES，其他类型直接拒绝。</p>
     *
     * @param value 类型名称
     * @return 配置类型
     */
    public static ConfigType fromSupported(String value) {
        if (value == null || value.trim().isEmpty()) {
            return YAML;
        }
        String normalized = value.trim();
        for (ConfigType type : SUPPORTED) {
            if (type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                "仅支持 YAML、JSON、PROPERTIES 格式: " + value);
    }
}
