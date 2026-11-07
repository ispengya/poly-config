package com.ispengya.polyconfig.core.raft;

/**
 * Raft 节点角色（节点内部状态机角色，状态上报时映射到 {@code NodeRole}）。
 */
public enum RaftRole {

    /**
     * 跟随者：接受 leader 心跳与复制，超时后转为候选者。
     */
    FOLLOWER,

    /**
     * 候选者：本 term 已自投并正在拉票。
     */
    CANDIDATE,

    /**
     * 领导者：唯一写入口，负责复制与提交。
     */
    LEADER
}
