package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.consistency.ClusterMember;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Distro 分区归属：分区（namespace + group）→ owner 节点。
 *
 * <p>全量副本模型下，分片只切写责任：owner 是分区的唯一写入口和
 * per-key 版本分配者，数据本身在每个节点都有完整副本。</p>
 *
 * <p>归属计算：{@code floorMod(hash(namespace + ' ' + group), 成员数)}，
 * 成员列表按 nodeId 排序后取下标。成员为静态配置，所有节点输入一致、
 * 计算结果一致，无须协调。hash 使用 {@link String#hashCode()}——
 * 其算法由 Java 语言规范固定，跨 JVM、跨版本稳定。</p>
 */
public final class DistroAssignments {

    /**
     * 按 nodeId 排序的成员列表。
     */
    private final List<ClusterMember> members;

    /**
     * nodeId → 成员，用于校验本节点在成员列表中。
     */
    private final Map<String, ClusterMember> byNodeId;

    /**
     * 创建分区归属表。
     *
     * @param members 静态成员列表，不可为空
     */
    public DistroAssignments(List<ClusterMember> members) {
        if (members == null || members.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "Distro 成员列表不能为空");
        }
        List<ClusterMember> sorted = new ArrayList<>(members);
        sorted.sort(Comparator.comparing(ClusterMember::getNodeId));
        this.members = Collections.unmodifiableList(sorted);
        this.byNodeId = new HashMap<>();
        for (ClusterMember member : sorted) {
            if (byNodeId.put(member.getNodeId(), member) != null) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "成员节点 ID 重复: " + member.getNodeId());
            }
        }
    }

    /**
     * 查询分区 owner。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @return owner 成员
     */
    public ClusterMember ownerOf(String namespace, String group) {
        if (namespace == null || group == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "namespace 与 group 不能为空");
        }
        int hash = (namespace + ' ' + group).hashCode();
        return members.get(Math.floorMod(hash, members.size()));
    }

    /**
     * 查询配置键所属分区的 owner。
     *
     * @param key 配置键
     * @return owner 成员
     */
    public ClusterMember ownerOf(ConfigKey key) {
        return ownerOf(key.getNamespace(), key.getGroup());
    }

    /**
     * 判断某节点是否为本分区 owner。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param nodeId 节点 ID
     * @return 是否为 owner
     */
    public boolean isOwner(String namespace, String group, String nodeId) {
        return ownerOf(namespace, group).getNodeId().equals(nodeId);
    }

    /**
     * 返回指定节点在成员列表中的配置。
     *
     * @param nodeId 节点 ID
     * @return 成员配置
     */
    public ClusterMember member(String nodeId) {
        ClusterMember member = byNodeId.get(nodeId);
        if (member == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "节点不在成员列表中: " + nodeId);
        }
        return member;
    }

    /**
     * 返回按 nodeId 排序的成员列表。
     *
     * @return 成员列表
     */
    public List<ClusterMember> members() {
        return members;
    }
}
