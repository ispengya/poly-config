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
    REPLICA
}
