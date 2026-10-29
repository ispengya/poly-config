package com.ispengya.polyconfig.common.model;

import com.ispengya.polyconfig.common.util.Md5Utils;

import java.util.Objects;

/**
 * 配置项当前版本。
 *
 * <p>不可变对象；md5 用于客户端比对和增量同步。</p>
 */
public final class ConfigEntry {

    /**
     * 配置键。
     */
    private final ConfigKey key;

    /**
     * 配置内容。
     */
    private final String content;

    /**
     * 内容类型。
     */
    private final ConfigType type;

    /**
     * 内容 MD5。
     */
    private final String md5;

    /**
     * 条目逻辑版本。
     */
    private final long version;

    /**
     * 集群变更序号。
     */
    private final long revision;

    /**
     * 创建时间。
     */
    private final long createdTime;

    /**
     * 最后更新时间。
     */
    private final long modifiedTime;

    /**
     * 最后操作人。
     */
    private final String operator;

    /**
     * 创建配置条目。
     *
     * @param key 配置键
     * @param content 配置内容
     * @param type 内容类型
     * @param md5 内容 MD5，为空时自动计算
     * @param version 条目逻辑版本
     * @param revision 集群变更序号
     * @param createdTime 创建时间
     * @param modifiedTime 最后更新时间
     * @param operator 最后操作人
     */
    public ConfigEntry(ConfigKey key, String content, ConfigType type, String md5,
                       long version, long revision, long createdTime, long modifiedTime,
                       String operator) {
        this.key = Objects.requireNonNull(key, "key");
        this.content = content == null ? "" : content;
        this.type = type == null ? ConfigType.TEXT : type;
        this.md5 = md5 == null ? Md5Utils.md5Hex(this.content) : md5;
        this.version = version;
        this.revision = revision;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
        this.operator = operator == null ? "" : operator;
    }

    public ConfigKey getKey() {
        return key;
    }

    public String getContent() {
        return content;
    }

    public ConfigType getType() {
        return type;
    }

    public String getMd5() {
        return md5;
    }

    public long getVersion() {
        return version;
    }

    public long getRevision() {
        return revision;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public String getOperator() {
        return operator;
    }
}
