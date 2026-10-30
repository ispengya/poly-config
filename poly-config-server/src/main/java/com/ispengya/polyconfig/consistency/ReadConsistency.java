package com.ispengya.polyconfig.consistency;

/**
 * 配置读取一致性级别。
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
