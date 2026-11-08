package com.ispengya.polyconfig.consistency;

/**
 * 配置读取一致性级别（服务端协议与客户端 SDK 共用，D8）。
 *
 * <p>默认 {@link #LOCAL}（任意节点本地直读）；{@link #LINEARIZABLE} 走
 * read-index（CP）或 owner 转发（AP），返回值不回退。</p>
 */
public enum ReadConsistency {

    /**
     * 读取本地状态，允许返回旧值。
     */
    LOCAL,

    /**
     * 读取已提交的最新状态；单机模式下等价于 {@link #LOCAL}。
     */
    LINEARIZABLE
}
