package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.raft.RaftProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CP 端到端测试（CP-2/CP-3/CP-4）：单节点写通与重启恢复、三节点选主与
 * 换代、写转发与故障恢复、线性读、InstallSnapshot 追平与滚动重启收敛。
 *
 * <p>端口段 18866-18868 / 19866-19868，与 AP 测试隔离；选举超时调小
 * （300ms 基数、50ms 心跳）让换代在秒级内可观测。</p>
 */
public class CpClusterIntegrationTest {

    private static final String SINGLE_MEMBER = "node-1=127.0.0.1:18866:19866";

    private static final String THREE_MEMBERS = "node-1=127.0.0.1:18866:19866,"
            + "node-2=127.0.0.1:18867:19867,"
            + "node-3=127.0.0.1:18868:19868";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldServeAsSingleNodeInCpClusterAndSurviveRestart() throws Exception {
        Path dataDir = folder.newFolder("cp-single").toPath();
        PolyConfigServer server = startCpServer(dataDir, "node-1", SINGLE_MEMBER, 18866, 19866);
        try {
            awaitLeader(server, "node-1", 5000L);

            Response publish = post(server.getHttpPort(), "/v1/cs/configs",
                    publishBody("app.yml", "v1", null));
            assertEquals(200, publish.status);
            assertEquals(0, JSON.parseObject(publish.body).getIntValue("code"));

            Response read = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals("v1", JSON.parseObject(read.body)
                    .getJSONObject("data").getString("content"));

            // CP 下乐观锁语义保持：-1 表示必须不存在 → 409
            Response conflict = post(server.getHttpPort(), "/v1/cs/configs",
                    publishBody("app.yml", "v2", -1L));
            assertEquals(409, conflict.status);

            Response cluster = get(server.getHttpPort(), "/v1/cs/cluster");
            JSONObject clusterData = JSON.parseObject(cluster.body).getJSONObject("data");
            assertEquals("CP", clusterData.getString("mode"));
            JSONObject self = findByNodeId(clusterData.getJSONArray("nodes"), "node-1");
            assertEquals("LEADER", self.getString("role"));
            assertTrue("term 应 ≥1", self.getLongValue("term") >= 1);
        } finally {
            server.stop();
        }

        // 单成员恢复：日志即权威，构造期即 apply——重启后无需等选主即可读到已应答写
        PolyConfigServer restarted = startCpServer(dataDir, "node-1", SINGLE_MEMBER, 18866, 19866);
        try {
            Response read = get(restarted.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, read.status);
            assertEquals("v1", JSON.parseObject(read.body)
                    .getJSONObject("data").getString("content"));
            // 重新当选后继续可写（version = raft index：no-op 占 1 号，首写 version=2）
            awaitLeader(restarted, "node-1", 5000L);
            Response publish2 = post(restarted.getHttpPort(), "/v1/cs/configs",
                    publishBody("app.yml", "v2", 2L));
            assertEquals(200, publish2.status);
        } finally {
            restarted.stop();
        }
    }

    @Test
    public void shouldElectSingleLeaderInThreeNodeClusterAndReElect() throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        try {
            servers.add(startCpServer(folder.newFolder("cp3-1").toPath(),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(folder.newFolder("cp3-2").toPath(),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(folder.newFolder("cp3-3").toPath(),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            LeaderSnapshot first = awaitExactlyOneLeader(servers, nodeIds, 8000L);

            // 杀 leader → 存活节点换代（term 更高；随机超时下首个 leader 可能是任一节点）
            servers.get(first.index).stop();
            List<PolyConfigServer> alive = new ArrayList<>();
            List<String> aliveIds = new ArrayList<>();
            for (int i = 0; i < servers.size(); i++) {
                if (i != first.index) {
                    alive.add(servers.get(i));
                    aliveIds.add(nodeIds.get(i));
                }
            }
            LeaderSnapshot second = awaitExactlyOneLeader(alive, aliveIds, 8000L);
            assertTrue("新 leader term 应更高: first=" + first.term + ", now=" + second.term,
                    second.term > first.term);
            assertTrue("新 leader 应为存活节点之一: " + second.leaderId + " vs dead "
                    + first.leaderId, !second.leaderId.equals(first.leaderId));
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    // ==================== CP-3 复制与提交 ====================

    @Test
    public void shouldWriteViaAnyNodeAndReadFromAllNodes() throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        try {
            servers.add(startCpServer(folder.newFolder("cp3w-1").toPath(),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(folder.newFolder("cp3w-2").toPath(),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(folder.newFolder("cp3w-3").toPath(),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            // 写打到 follower（非 leader 节点）→ RAFT_FORWARD 到 leader 提交
            int followerIndex = (leader.index + 1) % servers.size();
            PolyConfigServer follower = servers.get(followerIndex);
            Response publish = post(follower.getHttpPort(), "/v1/cs/configs",
                    publishBody("cp3.yml", "via-follower", null));
            assertEquals(200, publish.status);
            assertEquals(0, JSON.parseObject(publish.body).getIntValue("code"));

            // 全集群可读：三个节点各自 LOCAL 读，轮询直到复制+apply 追平
            for (PolyConfigServer server : servers) {
                awaitContent(server, "cp3.yml", "via-follower", 5000L);
            }

            // 409 经 follower 转发照样生效（leader 侧 D5 校验）
            Response conflict = post(follower.getHttpPort(), "/v1/cs/configs",
                    publishBody("cp3.yml", "again", -1L));
            assertEquals(409, conflict.status);
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    @Test
    public void shouldRejectWritesWhenMajorityDownAndRecoverWithoutLoss() throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<Path> dataDirs = new ArrayList<>();
        try {
            dataDirs.add(folder.newFolder("cp3m-1").toPath());
            dataDirs.add(folder.newFolder("cp3m-2").toPath());
            dataDirs.add(folder.newFolder("cp3m-3").toPath());
            servers.add(startCpServer(dataDirs.get(0),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(dataDirs.get(1),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(dataDirs.get(2),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            Response publish = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("keep.yml", "before-down", null));
            assertEquals(200, publish.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "keep.yml", "before-down", 5000L);
            }

            // 杀掉多数派（两个 follower）：leader 孤掌难鸣 → 写不可用（503）
            List<Integer> stoppedIndexes = new ArrayList<>();
            for (int i = 0; i < servers.size(); i++) {
                if (i != leader.index) {
                    killServer(servers.get(i));
                    stoppedIndexes.add(i);
                }
            }
            Response unavailable = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("keep.yml", "while-down", null));
            assertEquals("多数派不可用时写应 503", 503, unavailable.status);

            // 恢复多数派：一条不丢 + 自愈可写
            for (int k = 0; k < stoppedIndexes.size(); k++) {
                servers.set(stoppedIndexes.get(k), startCpServer(
                        dataDirs.get(stoppedIndexes.get(k)),
                        nodeIds.get(stoppedIndexes.get(k)), THREE_MEMBERS,
                        18866 + stoppedIndexes.get(k), 19866 + stoppedIndexes.get(k)));
            }
            // 恢复后终态：宕机期间超时的 while-down 写（index=3）也照常提交
            // （前缀提交蕴含 before-down 未丢）；内容确定为 while-down
            for (PolyConfigServer server : servers) {
                awaitContent(server, "keep.yml", "while-down", 8000L);
            }
            // 对准 version=3（while-down 的 index）写入 → 200，同时断言版本链正确
            Response recovered = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("keep.yml", "after-recovery", 3L));
            assertEquals("恢复后 version 应为 3（while-down 写已提交）, body="
                    + recovered.body, 200, recovered.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "keep.yml", "after-recovery", 8000L);
            }
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    @Test
    public void shouldEnforceInFlightWatermarkWhileMajorityDown() throws Exception {
        // D5：多数派宕机时写 A 追加未提交（在途），写 B 的 expectedVersion
        // 必须对着"已 apply + 在途叠加"判定——-1（必须不存在）应 409，
        // 对准 A 的 version 应放行（而非 409），随后超时 503。
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<Path> dataDirs = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                dataDirs.add(folder.newFolder("cp3d-" + (i + 1)).toPath());
            }
            servers.add(startCpServer(dataDirs.get(0),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(dataDirs.get(1),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(dataDirs.get(2),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            PolyConfigServer leaderServer = servers.get(leader.index);

            // 杀两个 follower：leader 无法取得多数派，写进入在途状态
            for (int i = 0; i < servers.size(); i++) {
                if (i != leader.index) {
                    killServer(servers.get(i));
                }
            }

            // 写 A（no-op 占 1 号 → A 为 index 2 / version 2）：本地追加后挂起
            Response writeA = post(leaderServer.getHttpPort(), "/v1/cs/configs",
                    publishBody("d5.yml", "d5-content", null));
            assertEquals("多数派不可用时写 A 应 503（条目留在日志里在途）",
                    503, writeA.status);

            // 写 B1：expectedVersion=-1（必须不存在）→ 叠加水位看到在途 A → 409
            long start = System.currentTimeMillis();
            Response writeB1 = post(leaderServer.getHttpPort(), "/v1/cs/configs",
                    publishBody("d5.yml", "b1", -1L));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals("在途 A 应被叠加水位挡住 → 409, body=" + writeB1.body,
                    409, writeB1.status);
            assertTrue("409 应即时返回而非等到提交超时: " + elapsed + "ms", elapsed < 2000L);

            // 写 B2：expectedVersion=2（正是 A 分配的 version）→ 叠加判定通过，
            // 追加后同样因多数派不可用超时 503（若水位缺失会先撞 -1 视图报 409）
            Response writeB2 = post(leaderServer.getHttpPort(), "/v1/cs/configs",
                    publishBody("d5.yml", "d5-content", 2L));
            assertEquals("对准在途 version 的写应放行后超时（而非 409）",
                    503, writeB2.status);

            // 恢复多数派：在途条目照常提交（A 与 B2 都在日志里，后写覆盖）
            List<PolyConfigServer> restarted = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                if (i != leader.index) {
                    servers.set(i, startCpServer(dataDirs.get(i), nodeIds.get(i),
                            THREE_MEMBERS, 18866 + i, 19866 + i));
                    restarted.add(servers.get(i));
                }
            }
            for (PolyConfigServer server : servers) {
                awaitContent(server, "d5.yml", "d5-content", 8000L);
            }
            Response read = get(leaderServer.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=d5.yml");
            JSONObject data = JSON.parseObject(read.body).getJSONObject("data");
            assertTrue("最终 version 应为 3（B2 的 index，A=2 已提交被覆盖）: "
                            + data.getLongValue("version"),
                    data.getLongValue("version") >= 3L);
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    @Test
    public void shouldFireChangeListenerOnEveryNodeWhenWritten() throws Exception {
        // apply 即推送钩子：写一次，三个节点的状态机 apply 都要触发监听
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<java.util.concurrent.CountDownLatch> latches = new ArrayList<>();
        try {
            servers.add(startCpServer(folder.newFolder("cp3p-1").toPath(),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(folder.newFolder("cp3p-2").toPath(),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(folder.newFolder("cp3p-3").toPath(),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");
            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);

            for (PolyConfigServer server : servers) {
                java.util.concurrent.CountDownLatch latch =
                        new java.util.concurrent.CountDownLatch(1);
                latches.add(latch);
                server.getConfigService().addChangeListener(event -> latch.countDown());
            }

            int followerIndex = (leader.index + 1) % servers.size();
            Response publish = post(servers.get(followerIndex).getHttpPort(),
                    "/v1/cs/configs", publishBody("push.yml", "push-me", null));
            assertEquals(200, publish.status);

            for (int i = 0; i < servers.size(); i++) {
                assertTrue("节点 " + nodeIds.get(i) + " 应触发变更监听（apply 即推送）",
                        latches.get(i).await(5, java.util.concurrent.TimeUnit.SECONDS));
            }
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    // ==================== CP-4 线性读 ====================

    @Test
    public void shouldServeLinearizableReadAndFailInsteadOfStaleDuringElection()
            throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<Path> dataDirs = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                dataDirs.add(folder.newFolder("cp4r-" + (i + 1)).toPath());
            }
            servers.add(startCpServer(dataDirs.get(0),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(dataDirs.get(1),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(dataDirs.get(2),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "lin.yml");
            Response publish = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("lin.yml", "lin-v1", null));
            assertEquals(200, publish.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "lin.yml", "lin-v1", 5000L);
            }

            // 正常态：leader 本机线性读 + follower 经 RAFT_READ 转发线性读 → lin-v1
            assertEquals("lin-v1", readLinearizable(servers.get(leader.index), key));
            int followerIndex = (leader.index + 1) % servers.size();
            assertEquals("follower 转发线性读应读到 lin-v1",
                    "lin-v1", readLinearizable(servers.get(followerIndex), key));

            // 杀多数派：leader 凑不齐确认轮 → 线性读失败（UNAVAILABLE），不回退读旧值
            for (int i = 0; i < servers.size(); i++) {
                if (i != leader.index) {
                    killServer(servers.get(i));
                }
            }
            try {
                readLinearizable(servers.get(leader.index), key);
                fail("多数派不可用时线性读应失败而非返回旧值");
            } catch (PolyConfigException e) {
                assertEquals(ErrorCode.UNAVAILABLE, e.getErrorCode());
            }

            // 恢复多数派：线性读恢复可用，值仍为 lin-v1（不回退、不丢失）
            for (int i = 0; i < servers.size(); i++) {
                if (i != leader.index) {
                    servers.set(i, startCpServer(dataDirs.get(i), nodeIds.get(i),
                            THREE_MEMBERS, 18866 + i, 19866 + i));
                }
            }
            awaitExactlyOneLeader(servers, nodeIds, 8000L);
            assertEquals("lin-v1", readLinearizable(servers.get(leader.index), key));

            // 杀 leader：选举窗口内线性读要么失败、要么读到 lin-v1（绝不更旧），
            // 最终在新 leader 当选后成功
            killServer(servers.get(leader.index));
            String electionWindowValue = null;
            long deadline = System.currentTimeMillis() + 8000L;
            while (System.currentTimeMillis() < deadline) {
                try {
                    electionWindowValue = readLinearizable(
                            servers.get((leader.index + 1) % servers.size()), key);
                    break;
                } catch (PolyConfigException e) {
                    // 选举窗口内只允许"不可用/非 leader"两类暂态失败，绝不返回旧值
                    assertTrue("选举窗口内只允许 UNAVAILABLE/NOT_LEADER: "
                                    + e.getErrorCode() + ", " + e.getMessage(),
                            e.getErrorCode() == ErrorCode.UNAVAILABLE
                                    || e.getErrorCode() == ErrorCode.NOT_LEADER);
                    Thread.sleep(50L);
                }
            }
            assertNotNull("换代后线性读应最终成功", electionWindowValue);
            assertEquals("选举窗口线性读不得回退", "lin-v1", electionWindowValue);

            // 新 leader 写 lin-v2 → 线性读必须读到 v2（≥ 之前的读，单调不回退）
            int survivorIndex = (leader.index + 1) % servers.size();
            Response publish2 = post(servers.get(survivorIndex).getHttpPort(),
                    "/v1/cs/configs", publishBody("lin.yml", "lin-v2", null));
            assertEquals(200, publish2.status);
            assertEquals("lin-v2", readLinearizable(servers.get(survivorIndex), key));
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    // ==================== CP-4 InstallSnapshot 与滚动重启 ====================

    @Test
    public void shouldCatchUpRestartedFollowerViaInstallSnapshot() throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<Path> dataDirs = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                dataDirs.add(folder.newFolder("cp4s-" + (i + 1)).toPath());
            }
            // 快照阈值 5：两批写各自触发一次快照与日志压缩
            servers.add(startCpServer(dataDirs.get(0),
                    "node-1", THREE_MEMBERS, 18866, 19866, 5));
            nodeIds.add("node-1");
            servers.add(startCpServer(dataDirs.get(1),
                    "node-2", THREE_MEMBERS, 18867, 19867, 5));
            nodeIds.add("node-2");
            servers.add(startCpServer(dataDirs.get(2),
                    "node-3", THREE_MEMBERS, 18868, 19868, 5));
            nodeIds.add("node-3");

            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            int followerIndex = (leader.index + 1) % servers.size();

            // 第一批 5 写（index 2..6）：各节点快照压到 6；
            // early.yml 落在第一批且不被覆盖，用于验证快照承载的完整状态
            Response early = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("early.yml", "early-v1", null));
            assertEquals(200, early.status);
            for (int i = 1; i <= 4; i++) {
                Response publish = post(servers.get(leader.index).getHttpPort(),
                        "/v1/cs/configs", publishBody("snap.yml", "batch1-" + i, null));
                assertEquals(200, publish.status);
            }
            for (PolyConfigServer server : servers) {
                awaitContent(server, "snap.yml", "batch1-4", 8000L);
                awaitContent(server, "early.yml", "early-v1", 8000L);
            }

            // 杀 follower：它停在第一批；leader 继续写并把日志压缩到它够不着的边界
            killServer(servers.get(followerIndex));
            for (int i = 6; i <= 10; i++) {
                Response publish = post(servers.get(leader.index).getHttpPort(),
                        "/v1/cs/configs", publishBody("snap.yml", "batch2-" + i, null));
                assertEquals(200, publish.status);
            }
            for (int i = 0; i < servers.size(); i++) {
                if (i != followerIndex) {
                    awaitContent(servers.get(i), "snap.yml", "batch2-10", 8000L);
                }
            }
            RaftProtocol leaderProtocol =
                    (RaftProtocol) servers.get(leader.index).getProtocol();
            assertTrue("leader 快照位点应越过被杀 follower 的日志尾",
                    leaderProtocol.raftNode().snapshotIndex() >= 11);

            // 重启 follower：经 leader 的 InstallSnapshot 一发追平第二批，
            // 且快照承载的完整状态（第一批的 early.yml）同样可见
            servers.set(followerIndex, startCpServer(dataDirs.get(followerIndex),
                    nodeIds.get(followerIndex), THREE_MEMBERS,
                    18866 + followerIndex, 19866 + followerIndex, 5));
            PolyConfigServer restarted = servers.get(followerIndex);
            awaitContent(restarted, "snap.yml", "batch2-10", 15000L);
            awaitContent(restarted, "early.yml", "early-v1", 5000L);
            RaftProtocol restartedProtocol = (RaftProtocol) restarted.getProtocol();
            assertTrue("重启 follower 应经 InstallSnapshot 安装 leader 快照（追平走快照路径）",
                    restartedProtocol.raftNode().snapshotIndex() >= 11);

            // 追平后继续可写可读（version 链不因快照中断）
            Response publish = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("snap.yml", "after-snap", 11L));
            assertEquals("快照压缩后 version 链应延续（batch2-10 的 index=11）, body="
                    + publish.body, 200, publish.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "snap.yml", "after-snap", 8000L);
            }
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    @Test
    public void shouldConvergeAfterRollingRestart() throws Exception {
        List<PolyConfigServer> servers = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        List<Path> dataDirs = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                dataDirs.add(folder.newFolder("cp4roll-" + (i + 1)).toPath());
            }
            servers.add(startCpServer(dataDirs.get(0),
                    "node-1", THREE_MEMBERS, 18866, 19866));
            nodeIds.add("node-1");
            servers.add(startCpServer(dataDirs.get(1),
                    "node-2", THREE_MEMBERS, 18867, 19867));
            nodeIds.add("node-2");
            servers.add(startCpServer(dataDirs.get(2),
                    "node-3", THREE_MEMBERS, 18868, 19868));
            nodeIds.add("node-3");

            awaitExactlyOneLeader(servers, nodeIds, 8000L);
            Response publish = post(servers.get(0).getHttpPort(),
                    "/v1/cs/configs", publishBody("roll.yml", "roll-v1", null));
            assertEquals(200, publish.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "roll.yml", "roll-v1", 8000L);
            }

            // 滚动重启：逐个杀+起，每步都恢复唯一 leader 与已提交数据
            for (int i = 0; i < servers.size(); i++) {
                killServer(servers.get(i));
                servers.set(i, startCpServer(dataDirs.get(i), nodeIds.get(i),
                        THREE_MEMBERS, 18866 + i, 19866 + i));
                awaitExactlyOneLeader(servers, nodeIds, 10000L);
                awaitContent(servers.get(i), "roll.yml", "roll-v1", 10000L);
            }

            // 全部滚动完：一条不丢、集群唯一 leader、继续可写
            for (PolyConfigServer server : servers) {
                awaitContent(server, "roll.yml", "roll-v1", 8000L);
            }
            LeaderSnapshot leader = awaitExactlyOneLeader(servers, nodeIds, 8000L);
            Response publish2 = post(servers.get(leader.index).getHttpPort(),
                    "/v1/cs/configs", publishBody("roll.yml", "roll-v2", 2L));
            assertEquals("滚动重启后 version 链应延续, body=" + publish2.body,
                    200, publish2.status);
            for (PolyConfigServer server : servers) {
                awaitContent(server, "roll.yml", "roll-v2", 8000L);
            }
        } finally {
            for (PolyConfigServer server : servers) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
            }
        }
    }

    // ==================== 辅助 ====================

    private static String readLinearizable(PolyConfigServer server, ConfigKey key) {
        RaftProtocol protocol = (RaftProtocol) server.getProtocol();
        Optional<ConfigEntry> entry = protocol.read(key, ReadConsistency.LINEARIZABLE);
        return entry.map(ConfigEntry::getContent).orElse(null);
    }

    private static PolyConfigServer startCpServer(Path dataDir, String nodeId,
                                                   String members, int httpPort,
                                                   int remotingPort) throws IOException {
        return startCpServer(dataDir, nodeId, members, httpPort, remotingPort, 10000);
    }

    private static PolyConfigServer startCpServer(Path dataDir, String nodeId,
                                                   String members, int httpPort,
                                                   int remotingPort,
                                                   int snapshotThreshold) throws IOException {
        PolyConfigServer server = new PolyConfigServer(ServerOptions.builder()
                .host("127.0.0.1")
                .advertiseHost("127.0.0.1")
                .httpPort(httpPort)
                .remotingPort(remotingPort)
                .dataDir(dataDir)
                .nodeId(nodeId)
                .clusterId("cp-test")
                .mode("cp")
                .clusterMembers(members)
                .snapshotThreshold(snapshotThreshold)
                .raftElectionTimeoutMillis(300L)
                .raftHeartbeatMillis(50L)
                .build());
        server.start();
        return server;
    }

    private static void awaitLeader(PolyConfigServer server, String nodeId, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Response cluster = get(server.getHttpPort(), "/v1/cs/cluster");
            JSONObject data = JSON.parseObject(cluster.body).getJSONObject("data");
            JSONObject self = findByNodeId(data.getJSONArray("nodes"), nodeId);
            if ("LEADER".equals(self.getString("role"))) {
                return;
            }
            Thread.sleep(50L);
        }
        fail("等待 " + nodeId + " 当选 leader 超时");
    }

    private static LeaderSnapshot awaitExactlyOneLeader(List<PolyConfigServer> servers,
                                                        List<String> nodeIds,
                                                        long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            // CP-2 视角约定：每个节点自报真实角色，对端为静态 FOLLOWER——
            // 全部视图里恰好一个 LEADER ⟺ 集群里恰好一个真实 leader
            LeaderSnapshot found = null;
            int leaderCount = 0;
            for (int i = 0; i < servers.size(); i++) {
                Response cluster = get(servers.get(i).getHttpPort(), "/v1/cs/cluster");
                JSONObject data = JSON.parseObject(cluster.body).getJSONObject("data");
                JSONObject self = findByNodeId(data.getJSONArray("nodes"), nodeIds.get(i));
                if ("LEADER".equals(self.getString("role"))) {
                    leaderCount++;
                    found = new LeaderSnapshot(i, self.getString("nodeId"),
                            self.getLongValue("term"));
                }
            }
            if (leaderCount == 1 && found != null) {
                return found;
            }
            Thread.sleep(50L);
        }
        fail("等待唯一 leader 超时");
        return null;
    }

    private static JSONObject findByNodeId(JSONArray nodes, String nodeId) {
        for (int i = 0; i < nodes.size(); i++) {
            JSONObject node = nodes.getJSONObject(i);
            if (nodeId.equals(node.getString("nodeId"))) {
                return node;
            }
        }
        fail("集群状态中缺少节点 " + nodeId);
        return null;
    }

    /**
     * 杀节点（测试故障注入）：先秒级关闭协议让 Raft 立刻退出，再走优雅停机
     * 清理网络——否则 stop 的 2~7 秒窗口内被停节点仍参与选举，会把真 leader
     * 拉下马，干扰"多数派不可用"类断言。协议 close 幂等，后续 stop 无害。
     */
    private static void killServer(PolyConfigServer server) {
        server.getConfigService().close();
        server.stop();
    }

    private static void awaitContent(PolyConfigServer server, String dataId,
                                     String expectedContent, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Response read = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=" + dataId);
            if (read.status == 200) {
                JSONObject data = JSON.parseObject(read.body).getJSONObject("data");
                if (data != null && expectedContent.equals(data.getString("content"))) {
                    return;
                }
            }
            Thread.sleep(50L);
        }
        fail("节点 " + server.getHttpPort() + " 上 " + dataId
                + " 未在 " + timeoutMillis + "ms 内读到内容 " + expectedContent);
    }

    private static String publishBody(String dataId, String content, Long expectedVersion) {
        JSONObject request = new JSONObject();
        request.put("namespace", "public");
        request.put("group", "DEFAULT_GROUP");
        request.put("dataId", dataId);
        request.put("content", content);
        request.put("type", "YAML");
        request.put("operator", "tester");
        if (expectedVersion != null) {
            request.put("expectedVersion", expectedVersion);
        }
        return request.toJSONString();
    }

    private static Response get(int port, String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(5000);
        return read(connection);
    }

    private static Response post(int port, String path, String body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(5000);
        connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        try (OutputStream out = connection.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return read(connection);
    }

    private static Response read(HttpURLConnection connection) throws IOException {
        int status = connection.getResponseCode();
        InputStream stream = status >= 400
                ? connection.getErrorStream() : connection.getInputStream();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        if (stream != null) {
            byte[] chunk = new byte[4096];
            int n;
            while ((n = stream.read(chunk)) >= 0) {
                buffer.write(chunk, 0, n);
            }
            stream.close();
        }
        return new Response(status, buffer.toString("UTF-8"));
    }

    private static final class LeaderSnapshot {
        private final int index;
        private final String leaderId;
        private final long term;

        private LeaderSnapshot(int index, String leaderId, long term) {
            this.index = index;
            this.leaderId = leaderId;
            this.term = term;
        }
    }

    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
