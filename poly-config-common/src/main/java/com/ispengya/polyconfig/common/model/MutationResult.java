package com.ispengya.polyconfig.common.model;

import java.util.Objects;

/**
 * 配置变更提交结果。
 */
public final class MutationResult {

    /**
     * 配置键。
     */
    private final ConfigKey key;

    /**
     * 提交后的条目版本。
     */
    private final long version;

    /**
     * 提交后的变更序号。
     */
    private final long revision;

    /**
     * 提交后内容的 MD5。
     */
    private final String md5;

    /**
     * 创建提交结果。
     *
     * @param key 配置键
     * @param version 条目版本
     * @param revision 变更序号
     * @param md5 内容 MD5
     */
    public MutationResult(ConfigKey key, long version, long revision, String md5) {
        this.key = Objects.requireNonNull(key, "key");
        this.version = version;
        this.revision = revision;
        this.md5 = md5 == null ? "" : md5;
    }

    public ConfigKey getKey() {
        return key;
    }

    public long getVersion() {
        return version;
    }

    public long getRevision() {
        return revision;
    }

    public String getMd5() {
        return md5;
    }
}
