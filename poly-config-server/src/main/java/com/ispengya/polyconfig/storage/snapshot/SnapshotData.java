package com.ispengya.polyconfig.storage.snapshot;

import java.util.List;

/**
 * 快照完整内容。
 *
 * <p>{@code logOffset} 是快照覆盖到的日志物理位点：回放从该位点继续，
 * 之前的完整段可以删除。{@code revision} 是全局变更序号，恢复后必须精确延续。</p>
 */
public class SnapshotData {

    /**
     * 快照格式版本。
     */
    public static final int FORMAT_VERSION = 1;

    private int formatVersion;
    private long revision;
    private long logOffset;
    private long savedAt;
    private List<SnapshotEntry> entries;
    private List<SnapshotHistory> histories;

    public SnapshotData() {
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public void setLogOffset(long logOffset) {
        this.logOffset = logOffset;
    }

    public long getSavedAt() {
        return savedAt;
    }

    public void setSavedAt(long savedAt) {
        this.savedAt = savedAt;
    }

    public List<SnapshotEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<SnapshotEntry> entries) {
        this.entries = entries;
    }

    public List<SnapshotHistory> getHistories() {
        return histories;
    }

    public void setHistories(List<SnapshotHistory> histories) {
        this.histories = histories;
    }
}
