package com.ispengya.polyconfig.consistency;

/**
 * 集群运行模式。
 */
public enum ProtocolMode {

    /**
     * 单机模式，无复制。
     */
    STANDALONE,

    /**
     * 强一致模式，由 Raft 实现。
     */
    CP,

    /**
     * 最终一致模式，由 Distro 实现。
     */
    AP
}
