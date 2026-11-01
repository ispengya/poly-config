package com.ispengya.polyconfig.common.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量订阅/对账请求。
 *
 * <p>一次往返携带多个配置键及其客户端侧版本，服务端只返回有差异的部分，
 * 替代客户端逐 key 的同步订阅请求。</p>
 */
public class BatchSubscribeRequest {

    /**
     * 订阅条目列表。
     */
    private List<SubscribeItem> targets = new ArrayList<>();

    public BatchSubscribeRequest() {
    }

    /**
     * 创建批量订阅请求。
     *
     * @param targets 订阅条目列表
     * @return 批量订阅请求
     */
    public static BatchSubscribeRequest of(List<SubscribeItem> targets) {
        BatchSubscribeRequest request = new BatchSubscribeRequest();
        request.targets = targets == null ? new ArrayList<>() : targets;
        return request;
    }

    public List<SubscribeItem> getTargets() {
        return targets;
    }

    public void setTargets(List<SubscribeItem> targets) {
        this.targets = targets;
    }
}
