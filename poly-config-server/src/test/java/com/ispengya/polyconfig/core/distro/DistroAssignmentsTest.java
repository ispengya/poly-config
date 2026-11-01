package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.consistency.ClusterMember;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distro 分区归属计算测试。
 */
public class DistroAssignmentsTest {

    private static final List<ClusterMember> MEMBERS = Arrays.asList(
            new ClusterMember("node-1", "127.0.0.1", 8848, 9848),
            new ClusterMember("node-2", "127.0.0.1", 8849, 9849),
            new ClusterMember("node-3", "127.0.0.1", 8850, 9850));

    @Test
    public void shouldMapEveryKeyToKnownMember() {
        DistroAssignments assignments = new DistroAssignments(MEMBERS);
        Set<String> nodeIds = new HashSet<>();
        for (ClusterMember member : MEMBERS) {
            nodeIds.add(member.getNodeId());
        }
        for (int i = 0; i < 200; i++) {
            String owner = assignments.ownerOf("ns-" + i, "group-" + (i % 7)).getNodeId();
            assertTrue("owner 必须是成员之一: " + owner, nodeIds.contains(owner));
        }
    }

    @Test
    public void shouldBeDeterministicAcrossInstances() {
        DistroAssignments first = new DistroAssignments(MEMBERS);
        // 打乱输入顺序也不影响结果：归属表内部按 nodeId 排序
        List<ClusterMember> shuffled = new ArrayList<>(MEMBERS);
        java.util.Collections.reverse(shuffled);
        DistroAssignments second = new DistroAssignments(shuffled);
        for (int i = 0; i < 50; i++) {
            assertEquals(first.ownerOf("public", "GROUP-" + i).getNodeId(),
                    second.ownerOf("public", "GROUP-" + i).getNodeId());
        }
    }

    @Test
    public void shouldDistributePartitionsAcrossAllMembers() {
        DistroAssignments assignments = new DistroAssignments(MEMBERS);
        Set<String> owners = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            owners.add(assignments.ownerOf("public", "app-" + i).getNodeId());
        }
        assertEquals("100 个分区应覆盖全部 3 个成员", 3, owners.size());
    }

    @Test
    public void shouldAnswerIsOwnerConsistently() {
        DistroAssignments assignments = new DistroAssignments(MEMBERS);
        String owner = assignments.ownerOf("prod", "payment").getNodeId();
        assertTrue(assignments.isOwner("prod", "payment", owner));
        for (ClusterMember member : MEMBERS) {
            if (!member.getNodeId().equals(owner)) {
                assertTrue(!assignments.isOwner("prod", "payment", member.getNodeId()));
            }
        }
    }

    @Test
    public void shouldRejectEmptyMembers() {
        try {
            new DistroAssignments(new ArrayList<ClusterMember>());
            fail("空成员列表应被拒绝");
        } catch (PolyConfigException expected) {
            // 预期
        }
    }

    @Test
    public void shouldRejectUnknownSelfNode() {
        DistroAssignments assignments = new DistroAssignments(MEMBERS);
        try {
            assignments.member("node-9");
            fail("不在成员列表中的节点应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue(expected.getMessage().contains("node-9"));
        }
    }
}
