package com.ispengya.polyconfig.common.protocol;

import java.util.List;

/**
 * AP 周期校验请求（副本 → owner，3xxx 内部协议）。
 *
 * <p>携带本节点持有的、目标成员拥有的分区全量摘要（key→version，含墓碑）；
 * owner 逐 key 比对后在应答体中返回增量（复用 {@link DistroSyncRequest}
 * 形态的命令记录，副本按与 {@code DISTRO_SYNC} 相同的幂等规则应用）。</p>
 */
public class DistroVerifyRequest {

    /**
     * 发送方节点 ID（异常日志定位用）。
     */
    private String sourceNodeId;

    /**
     * 目标成员拥有的分区摘要条目。
     */
    private List<VerifyEntry> entries;

    public DistroVerifyRequest() {
    }

    /**
     * 创建校验请求。
     *
     * @param sourceNodeId 发送方节点 ID
     * @param entries 摘要条目
     * @return 校验请求
     */
    public static DistroVerifyRequest of(String sourceNodeId, List<VerifyEntry> entries) {
        DistroVerifyRequest request = new DistroVerifyRequest();
        request.sourceNodeId = sourceNodeId;
        request.entries = entries;
        return request;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public void setSourceNodeId(String sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public List<VerifyEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<VerifyEntry> entries) {
        this.entries = entries;
    }
}
