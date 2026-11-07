package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CP-2 端到端测试：单节点 cp 写通与重启恢复、三节点选主与换代。
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

    // ==================== 辅助 ====================

    private static PolyConfigServer startCpServer(Path dataDir, String nodeId,
                                                   String members, int httpPort,
                                                   int remotingPort) throws IOException {
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
