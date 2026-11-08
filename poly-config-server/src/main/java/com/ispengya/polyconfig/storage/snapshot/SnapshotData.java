package com.ispengya.polyconfig.storage.snapshot;

import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 快照完整内容。
 *
 * <p>{@code logOffset} 是快照覆盖到的日志物理位点：回放从该位点继续，
 * 之前的完整段可以删除。{@code revision} 是全局变更序号，恢复后必须精确延续。</p>
 *
 * <p>CP 起携带 {@code lastIncludedIndex}/{@code lastIncludedTerm}（cp-design.md
 * §7）：快照视作该位置的一条伪日志条目，用于 InstallSnapshot 下发、回退
 * 匹配点校验与恢复起点（AP 快照二者为 0）。</p>
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
    /**
     * CP：快照覆盖到的最后一条 Raft 日志 index（含）；0 表示非 CP 快照。
     */
    private long lastIncludedIndex;
    /**
     * CP：快照覆盖到的最后一条 Raft 日志的 term。
     */
    private long lastIncludedTerm;
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

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(long lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
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

    /**
     * 快照条目还原为配置条目列表（恢复用）。
     *
     * @return 配置条目列表
     */
    public List<ConfigEntry> toEntries() {
        List<ConfigEntry> result = new ArrayList<>();
        if (entries == null) {
            return result;
        }
        for (SnapshotEntry row : entries) {
            ConfigKey key = new ConfigKey(row.getNamespace(), row.getGroup(), row.getDataId());
            result.add(new ConfigEntry(key, row.getContent(),
                    ConfigType.from(row.getType()), row.getMd5(),
                    row.getVersion(), row.getRevision(),
                    row.getCreatedTime(), row.getModifiedTime(), row.getOperator()));
        }
        return result;
    }

    /**
     * 快照历史还原为每 key 历史表（恢复用，新的在前）。
     *
     * @return 历史表
     */
    public Map<ConfigKey, List<HistoryEntry>> toHistories() {
        Map<ConfigKey, List<HistoryEntry>> result = new HashMap<>();
        if (histories == null) {
            return result;
        }
        for (SnapshotHistory history : histories) {
            ConfigKey key = new ConfigKey(history.getNamespace(), history.getGroup(),
                    history.getDataId());
            List<HistoryEntry> items = new ArrayList<>();
            if (history.getItems() != null) {
                for (SnapshotHistoryItem row : history.getItems()) {
                    items.add(new HistoryEntry(key, row.getRevision(),
                            ConfigOperation.valueOf(row.getOperation()), row.getContent(),
                            ConfigType.from(row.getType()),
                            row.getMd5(), row.getOperator(), row.getTimestamp()));
                }
            }
            result.put(key, items);
        }
        return result;
    }
}
