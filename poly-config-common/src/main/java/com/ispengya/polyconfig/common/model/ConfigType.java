package com.ispengya.polyconfig.common.model;

/**
 * 配置内容类型。
 */
public enum ConfigType {

    TEXT,
    JSON,
    YAML,
    PROPERTIES,
    XML,
    HTML;

    /**
     * 解析配置类型，无法识别时回退为 {@link #TEXT}。
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
}
