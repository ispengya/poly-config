package com.ispengya.polyconfig.storage.snapshot;

import java.util.List;

/**
 * 快照中单个配置键的历史（DTO）。
 *
 * <p>{@code items} 顺序与状态机一致：新的在前。</p>
 */
public class SnapshotHistory {

    private String namespace;
    private String group;
    private String dataId;
    private List<SnapshotHistoryItem> items;

    public SnapshotHistory() {
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public List<SnapshotHistoryItem> getItems() {
        return items;
    }

    public void setItems(List<SnapshotHistoryItem> items) {
        this.items = items;
    }
}
