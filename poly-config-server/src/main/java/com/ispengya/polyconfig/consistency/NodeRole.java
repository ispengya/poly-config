package com.ispengya.polyconfig.consistency;

/**
 * 节点在协议中的角色。
 */
public enum NodeRole {

    /**
     * 单机模式下的唯一节点。
     */
    STANDALONE,

    /**
     * CP 模式下的 leader。
     */
    LEADER,

    /**
     * CP 模式下的 follower。
     */
    FOLLOWER,

    /**
     * CP 模式下的候选者。
     */
    CANDIDATE,

    /**
     * AP 模式下某个分片的 owner。
     */
    OWNER,

    /**
     * AP 模式下某个分片的副本。
     */
    REPLICA,

    /**
     * AP 模式下的对等节点。
     *
     * <p>全量副本模型下每个节点既是若干分区的 owner、也是其余分区的副本，
     * 节点级角色无法用单一的 OWNER / REPLICA 描述，统一报告为 PEER；
     * 分区级的 owner 归属见 {@code DistroAssignments}。</p>
     */
    PEER
}
