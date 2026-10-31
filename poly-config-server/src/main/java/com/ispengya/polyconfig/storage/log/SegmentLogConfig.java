package com.ispengya.polyconfig.storage.log;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.nio.file.Path;

/**
 * 分段日志配置。
 */
public final class SegmentLogConfig {

    /**
     * 默认日志段大小：8 MiB。
     */
    public static final int DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024;

    /**
     * 默认单条记录上限：1 MiB。
     */
    public static final int DEFAULT_MAX_RECORD_SIZE = 1024 * 1024;

    /**
     * 日志根目录。
     */
    private final Path baseDir;

    /**
     * 单个日志段文件大小。
     */
    private final int segmentSize;

    /**
     * 单条记录上限。
     */
    private final int maxRecordSize;

    /**
     * 是否每次追加后立即刷盘。
     */
    private final boolean syncOnAppend;

    /**
     * 是否允许首个日志段起始 offset 非 0（存在快照、前缀段已删除时为 true）。
     */
    private final boolean allowNonZeroStart;

    private SegmentLogConfig(Builder builder) {
        this.baseDir = builder.baseDir;
        this.segmentSize = builder.segmentSize;
        this.maxRecordSize = builder.maxRecordSize;
        this.syncOnAppend = builder.syncOnAppend;
        this.allowNonZeroStart = builder.allowNonZeroStart;
    }

    /**
     * 创建配置构造器。
     *
     * @param baseDir 日志根目录
     * @return 构造器
     */
    public static Builder builder(Path baseDir) {
        return new Builder(baseDir);
    }

    public Path getBaseDir() {
        return baseDir;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public int getMaxRecordSize() {
        return maxRecordSize;
    }

    public boolean isSyncOnAppend() {
        return syncOnAppend;
    }

    public boolean isAllowNonZeroStart() {
        return allowNonZeroStart;
    }

    /**
     * 分段日志配置构造器。
     */
    public static final class Builder {

        /**
         * 日志根目录。
         */
        private final Path baseDir;

        /**
         * 单个日志段文件大小。
         */
        private int segmentSize = DEFAULT_SEGMENT_SIZE;

        /**
         * 单条记录上限。
         */
        private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;

        /**
         * 是否每次追加后立即刷盘。
         */
        private boolean syncOnAppend;

        /**
         * 是否允许首个日志段起始 offset 非 0。
         */
        private boolean allowNonZeroStart;

        private Builder(Path baseDir) {
            if (baseDir == null) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "baseDir 不能为空");
            }
            this.baseDir = baseDir;
        }

        public Builder segmentSize(int segmentSize) {
            if (segmentSize <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "segmentSize 必须大于 0");
            }
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder maxRecordSize(int maxRecordSize) {
            if (maxRecordSize <= 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "maxRecordSize 必须大于 0");
            }
            this.maxRecordSize = maxRecordSize;
            return this;
        }

        public Builder syncOnAppend(boolean syncOnAppend) {
            this.syncOnAppend = syncOnAppend;
            return this;
        }

        public Builder allowNonZeroStart(boolean allowNonZeroStart) {
            this.allowNonZeroStart = allowNonZeroStart;
            return this;
        }

        public SegmentLogConfig build() {
            return new SegmentLogConfig(this);
        }
    }
}
