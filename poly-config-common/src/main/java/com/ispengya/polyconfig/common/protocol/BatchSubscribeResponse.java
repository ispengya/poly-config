package com.ispengya.polyconfig.common.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量订阅/对账响应。
 *
 * <p>只包含与客户端声明版本有差异的键：{@code changed} 为内容有变更的完整视图，
 * {@code deleted} 为服务端已不存在但客户端声明持有的键。未变更的键不出现在响应中。</p>
 */
public class BatchSubscribeResponse {

    /**
     * 内容有变更的配置视图。
     */
    private List<ConfigView> changed = new ArrayList<>();

    /**
     * 服务端已删除、客户端应清除的配置键。
     */
    private List<SubscribeItem> deleted = new ArrayList<>();

    public BatchSubscribeResponse() {
    }

    public List<ConfigView> getChanged() {
        return changed;
    }

    public void setChanged(List<ConfigView> changed) {
        this.changed = changed == null ? new ArrayList<>() : changed;
    }

    public List<SubscribeItem> getDeleted() {
        return deleted;
    }

    public void setDeleted(List<SubscribeItem> deleted) {
        this.deleted = deleted == null ? new ArrayList<>() : deleted;
    }
}
