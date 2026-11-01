package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AP-1 DistroProtocol 骨架测试：委托链路可用，状态上报 AP 语义。
 */
public class DistroProtocolTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static List<ClusterMember> members() {
        // 使用隔离端口段，避免探测到本机上正在运行的演示集群
        return Arrays.asList(
                new ClusterMember("node-1", "127.0.0.1", 18848, 19848),
                new ClusterMember("node-2", "127.0.0.1", 18849, 19849),
                new ClusterMember("node-3", "127.0.0.1", 18850, 19850));
    }

    @Test
    public void shouldProposeReadAndReportApStatus() throws Exception {
        Path dataDir = folder.newFolder("ap-data").toPath();
        DistroProtocol protocol = new DistroProtocol(
                dataDir, "node-1", "test-cluster", 100, 0, members());
        try {
            protocol.setNodeAddress("127.0.0.1", 18848, 19848);
            protocol.start();

            ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "app.yml");
            MutationResult result = protocol.propose(new ConfigCommand(
                    ConfigOperation.PUBLISH, key, "port: 8080", ConfigType.YAML,
                    "tester", System.currentTimeMillis()));
            assertTrue(result.getRevision() > 0);
            assertEquals("port: 8080",
                    protocol.read(key, ReadConsistency.LOCAL).get().getContent());

            NodeStatus status = protocol.status();
            assertEquals(ProtocolMode.AP, status.getMode());
            assertEquals("node-1", status.getNodeId());
            assertEquals(NodeRole.PEER, status.getRole());
            assertEquals("127.0.0.1", status.getHost());
            assertEquals(18848, status.getPort());
            assertEquals(19848, status.getRemotingPort());
            assertEquals(3, status.getMemberCount());
            assertEquals(0L, status.getTerm());
            assertTrue(status.isHealthy());

            ClusterStatus cluster = protocol.clusterStatus();
            assertEquals("test-cluster", cluster.getClusterId());
            assertEquals("node-1", cluster.getCurrentNodeId());
            assertEquals(ProtocolMode.AP, cluster.getMode());
            assertEquals(3, cluster.getNodes().size());

            NodeStatus self = cluster.getNodes().get(0);
            assertEquals("node-1", self.getNodeId());
            assertTrue("本节点应健康", self.isHealthy());

            NodeStatus peer = cluster.getNodes().get(1);
            assertEquals("node-2", peer.getNodeId());
            assertEquals("127.0.0.1", peer.getHost());
            assertEquals(18849, peer.getPort());
            assertEquals(19849, peer.getRemotingPort());
            assertEquals(ProtocolMode.AP, peer.getMode());
            assertEquals(NodeRole.PEER, peer.getRole());
            assertFalse("peer 端口无监听，探测不成功，健康位为 false", peer.isHealthy());
        } finally {
            protocol.close();
        }
    }

    @Test
    public void shouldPersistDataAcrossRestart() throws Exception {
        Path dataDir = folder.newFolder("ap-restart").toPath();
        ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "app.yml");
        DistroProtocol first = new DistroProtocol(
                dataDir, "node-1", "test-cluster", 100, 0, members());
        try {
            first.propose(new ConfigCommand(
                    ConfigOperation.PUBLISH, key, "v1", ConfigType.YAML,
                    "tester", System.currentTimeMillis()));
        } finally {
            first.close();
        }
        DistroProtocol second = new DistroProtocol(
                dataDir, "node-1", "test-cluster", 100, 0, members());
        try {
            assertEquals("v1", second.read(key, ReadConsistency.LOCAL).get().getContent());
            assertEquals(ProtocolMode.AP, second.status().getMode());
        } finally {
            second.close();
        }
    }

    @Test
    public void shouldMarkPeerHealthyWhenPeerListening() throws Exception {
        Path dataDir = folder.newFolder("ap-probe").toPath();
        // 在 node-2 的长连接端口上挂一个纯 TCP 监听，模拟"节点在线"
        java.net.ServerSocket peerSocket = new java.net.ServerSocket(19849);
        DistroProtocol protocol = new DistroProtocol(
                dataDir, "node-1", "test-cluster", 100, 0, members());
        try {
            protocol.start();
            long deadline = System.currentTimeMillis() + 7000L;
            boolean healthy = false;
            while (System.currentTimeMillis() < deadline && !healthy) {
                healthy = protocol.clusterStatus().getNodes().get(1).isHealthy();
                if (!healthy) {
                    Thread.sleep(100L);
                }
            }
            assertTrue("探测到 peer 在线后健康位应点亮", healthy);
        } finally {
            protocol.close();
            peerSocket.close();
        }
    }

    @Test
    public void shouldRejectConstructionWhenSelfNotInMembers() throws Exception {
        Path dataDir = folder.newFolder("ap-noself").toPath();
        try {
            new DistroProtocol(dataDir, "node-9", "test-cluster", 100, 0, members());
            fail("本节点不在成员列表中应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含节点 ID: " + expected.getMessage(),
                    expected.getMessage().contains("node-9"));
        }
    }
}
