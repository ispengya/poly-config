package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.core.distro.DistroAssignments;
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
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 周期校验/反熵端到端测试（docs/ap-design.md §11 验收）：
 * 杀副本后重启收敛（含墓碑）、空闲集群健康位由校验点亮、杀 owner 后恢复写入。
 */
public class ApVerifyIntegrationTest {

    private static final String MEMBERS =
            "node-1=127.0.0.1:18854:19854,node-2=127.0.0.1:18855:19855";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldConvergeReplicaAfterRestartIncludingTombstone() throws Exception {
        Path dataDir1 = folder.newFolder("verify-node1").toPath();
        Path dataDir2 = folder.newFolder("verify-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18854, 19854, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18855, 19855, dataDir2);
        PolyConfigServer restarted = null;
        try {
            DistroAssignments assignments = assignments();
            boolean ownerIsNode1 = "node-1"
                    .equals(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            PolyConfigServer owner = ownerIsNode1 ? node1 : node2;
            PolyConfigServer nonOwner = ownerIsNode1 ? node2 : node1;
            int ownerHttp = owner.getHttpPort();
            int nonOwnerHttp = nonOwner.getHttpPort();

            // 初始：两个键都经复制到达副本
            assertEquals(200, post(ownerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v1", null)).status);
            assertEquals(200, post(ownerHttp, "/v1/cs/configs",
                    publishBody("doomed.yml", "v1", null)).status);
            waitUntil("初始数据应复制到副本", 8000L, () ->
                    read(nonOwnerHttp, "app.yml").contains("v1")
                            && read(nonOwnerHttp, "doomed.yml").contains("v1"));

            // 杀副本：期间 owner 推进（升级 app.yml、删除 doomed.yml），
            // 发往副本的 SYNC 全部失败丢弃——只靠重启后的校验补齐
            nonOwner.stop();
            assertEquals(200, post(ownerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v2", 1L)).status);
            Response deleted = delete(ownerHttp,
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP"
                            + "&dataId=doomed.yml&operator=tester");
            assertEquals(200, deleted.status);

            // 重启副本（同数据目录）：校验增量应补上升级与墓碑
            restarted = ownerIsNode1
                    ? startNode("node-2", 18855, 19855, dataDir2)
                    : startNode("node-1", 18854, 19854, dataDir1);
            int restartedHttp = restarted.getHttpPort();
            waitUntil("重启后的副本应通过校验拿到最新内容", 8000L, () ->
                    read(restartedHttp, "app.yml").contains("v2"));
            waitUntil("重启后的副本应通过校验删除墓碑键", 8000L, () -> {
                Response response = get(restartedHttp,
                        "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP"
                                + "&dataId=doomed.yml");
                return response.status == 404;
            });

            // 校验同时是探活：重启后的副本视角里 owner 应点亮健康位
            waitUntil("校验成功应点亮对端健康位", 8000L, () ->
                    allPeersHealthy(restartedHttp));
        } finally {
            if (restarted != null) {
                restarted.stop();
            }
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void shouldKeepClusterHealthyWhenIdle() throws Exception {
        Path dataDir1 = folder.newFolder("idle-node1").toPath();
        Path dataDir2 = folder.newFolder("idle-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18854, 19854, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18855, 19855, dataDir2);
        try {
            // 无任何写入：健康位只能由周期校验点亮（纯展示 TCP 探测已移除）
            waitUntil("空闲集群 node-1 视角应全员健康", 8000L, () ->
                    allPeersHealthy(node1.getHttpPort()));
            waitUntil("空闲集群 node-2 视角应全员健康", 8000L, () ->
                    allPeersHealthy(node2.getHttpPort()));
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void shouldRejectWritesWhileOwnerDownAndRecoverAfterRestart() throws Exception {
        Path dataDir1 = folder.newFolder("down-node1").toPath();
        Path dataDir2 = folder.newFolder("down-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18854, 19854, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18855, 19855, dataDir2);
        PolyConfigServer restartedOwner = null;
        try {
            DistroAssignments assignments = assignments();
            boolean ownerIsNode1 = "node-1"
                    .equals(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            PolyConfigServer owner = ownerIsNode1 ? node1 : node2;
            PolyConfigServer nonOwner = ownerIsNode1 ? node2 : node1;
            int ownerHttp = owner.getHttpPort();
            int nonOwnerHttp = nonOwner.getHttpPort();

            assertEquals(200, post(ownerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v1", null)).status);
            waitUntil("初始数据应复制到副本", 8000L, () ->
                    read(nonOwnerHttp, "app.yml").contains("v1"));

            // 杀 owner：该分区写不可用（任意节点转发都失败 → 503），读不受影响
            owner.stop();
            Response rejected = post(nonOwnerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v2", 1L));
            assertEquals(503, rejected.status);
            assertTrue("错误信息应带 owner 节点", rejected.body.contains("owner"));
            assertTrue("owner 宕机期间本地读应保持最后同步点",
                    read(nonOwnerHttp, "app.yml").contains("v1"));

            // 重启 owner（同数据目录）：本地恢复 + 校验对齐，写恢复
            restartedOwner = ownerIsNode1
                    ? startNode("node-1", 18854, 19854, dataDir1)
                    : startNode("node-2", 18855, 19855, dataDir2);
            assertEquals(200, post(nonOwnerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v2", 1L)).status);
            waitUntil("owner 恢复后写入应复制回副本", 8000L, () ->
                    read(nonOwnerHttp, "app.yml").contains("v2"));
        } finally {
            if (restartedOwner != null) {
                restartedOwner.stop();
            }
            node1.stop();
            node2.stop();
        }
    }

    private static DistroAssignments assignments() {
        return new DistroAssignments(Arrays.asList(
                new ClusterMember("node-1", "127.0.0.1", 18854, 19854),
                new ClusterMember("node-2", "127.0.0.1", 18855, 19855)));
    }

    /**
     * 集群状态里所有节点都健康（本节点恒健康，对端由校验点亮）。
     */
    private static boolean allPeersHealthy(int httpPort) throws IOException {
        Response response = get(httpPort, "/v1/cs/cluster");
        if (response.status != 200) {
            return false;
        }
        JSONArray nodes = JSON.parseObject(response.body)
                .getJSONObject("data").getJSONArray("nodes");
        if (nodes == null || nodes.isEmpty()) {
            return false;
        }
        for (int i = 0; i < nodes.size(); i++) {
            if (!nodes.getJSONObject(i).getBooleanValue("healthy")) {
                return false;
            }
        }
        return true;
    }

    private interface Condition {
        boolean met() throws IOException;
    }

    private static void waitUntil(String message, long deadlineMillis, Condition condition)
            throws Exception {
        long deadline = System.currentTimeMillis() + deadlineMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.met()) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError(message);
    }

    private static PolyConfigServer startNode(String nodeId, int httpPort, int remotingPort,
                                              Path dataDir) throws IOException {
        PolyConfigServer server = new PolyConfigServer(ServerOptions.builder()
                .host("127.0.0.1")
                .advertiseHost("127.0.0.1")
                .httpPort(httpPort)
                .remotingPort(remotingPort)
                .dataDir(dataDir)
                .nodeId(nodeId)
                .clusterId("ap-verify-test")
                .mode("ap")
                .clusterMembers(MEMBERS)
                .verifyIntervalMillis(1000L)
                .build());
        server.start();
        return server;
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

    /**
     * 读取配置内容；404 时返回空串，便于 waitUntil 条件表达"已删除"。
     */
    private static String read(int httpPort, String dataId) throws IOException {
        Response response = get(httpPort,
                "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=" + dataId);
        if (response.status == 404) {
            return "";
        }
        if (response.status != 200) {
            return "status-" + response.status;
        }
        return JSON.parseObject(response.body).getJSONObject("data").getString("content");
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

    private static Response delete(int port, String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("DELETE");
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(5000);
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

    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
