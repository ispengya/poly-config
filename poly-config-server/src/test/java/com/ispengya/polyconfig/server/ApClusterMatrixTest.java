package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 三节点集群测试矩阵（docs/ap-design.md §11 验收）：
 * 全链路写任意节点收敛+推送、状态采集聚合、滚动重启、跨节点并发乐观锁。
 */
public class ApClusterMatrixTest {

    private static final String[] IDS = {"node-1", "node-2", "node-3"};
    private static final int[] HTTP_PORTS = {18856, 18857, 18858};
    private static final int[] REMOTING_PORTS = {19856, 19857, 19858};
    private static final String MEMBERS =
            "node-1=127.0.0.1:18856:19856,node-2=127.0.0.1:18857:19857,"
                    + "node-3=127.0.0.1:18858:19858";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldConvergeAndPushAcrossThreeNodes() throws Exception {
        PolyConfigServer[] nodes = startAll("push");
        List<PolyConfigClient> clients = new ArrayList<>();
        try {
            DistroAssignments assignments = assignments();
            int ownerIndex = indexOf(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            // 客户端各连一个节点：分别覆盖 owner 本机推送、转发节点推送、纯副本推送
            CountDownLatch[] latches = new CountDownLatch[3];
            AtomicReference<String>[] pushed = new AtomicReference[3];
            for (int i = 0; i < 3; i++) {
                latches[i] = new CountDownLatch(1);
                pushed[i] = new AtomicReference<>();
                PolyConfigClient client = new PolyConfigClient(ClientOptions.builder()
                        .server("127.0.0.1:" + REMOTING_PORTS[i])
                        .snapshotDir(folder.newFolder("push-snap-" + i).toPath())
                        .requestTimeoutMillis(1000L)
                        .resyncIntervalMillis(60000L)
                        .build());
                final int index = i;
                client.addListener("DEFAULT_GROUP", "app.yml",
                        (dataId, group, value) -> {
                            pushed[index].set(value);
                            latches[index].countDown();
                        });
                clients.add(client);
            }
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 应收到订阅", 5000L, () -> {
                    Response subs = get(port, "/v1/cs/subscriptions");
                    return subs.status == 200
                            && JSON.parseObject(subs.body).getJSONObject("data")
                            .getIntValue("total") >= 1;
                });
            }

            // 写非 owner 节点：转发 → owner 落盘 → 复制/转发 apply 推送全体
            int writerIndex = (ownerIndex + 1) % 3;
            Response publish = post(HTTP_PORTS[writerIndex], "/v1/cs/configs",
                    publishBody("app.yml", "matrix-v1", null));
            assertEquals(200, publish.status);

            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 应通过复制收敛", 8000L, () ->
                        read(port, "app.yml").contains("matrix-v1"));
            }
            for (int i = 0; i < 3; i++) {
                assertTrue("连 node-" + (i + 1) + " 的客户端应收到推送",
                        latches[i].await(8, TimeUnit.SECONDS));
                assertEquals("matrix-v1", pushed[i].get());
            }
        } finally {
            for (PolyConfigClient client : clients) {
                client.close();
            }
            stopAll(nodes);
        }
    }

    @Test
    public void shouldExposeClusterDetailsFromEveryNode() throws Exception {
        PolyConfigServer[] nodes = startAll("status");
        try {
            // 三个分区（写入即存在），归属分散到各 owner
            assertEquals(200, post(HTTP_PORTS[0], "/v1/cs/configs",
                    publishBody("app.yml", "v1", null)).status);
            assertEquals(200, post(HTTP_PORTS[1], "/v1/cs/configs",
                    publishBodyInGroup("G0", "g0.yml", "v1")).status);
            assertEquals(200, post(HTTP_PORTS[2], "/v1/cs/configs",
                    publishBodyInGroup("G1", "g1.yml", "v1")).status);

            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 的状态采集应含对端校验时间", 8000L, () -> {
                    JSONArray details = detailsOf(get(port, "/v1/cs/cluster"));
                    if (details == null || details.size() != 3) {
                        return false;
                    }
                    for (int d = 0; d < details.size(); d++) {
                        JSONObject detail = details.getJSONObject(d);
                        JSONArray peers = detail.getJSONArray("peers");
                        if (peers == null || peers.size() != 2) {
                            return false;
                        }
                        for (int p = 0; p < peers.size(); p++) {
                            if (peers.getJSONObject(p).getLongValue("lastVerify") <= 0L) {
                                return false;
                            }
                        }
                        if (detail.getLongValue("anomalyCount") != 0L) {
                            return false;
                        }
                    }
                    return true;
                });

                JSONArray details = detailsOf(get(port, "/v1/cs/cluster"));
                assertNotNull(details);
                // 三个分区应恰好各被一个节点认领，并集完整
                List<String> owned = new ArrayList<>();
                for (int d = 0; d < details.size(); d++) {
                    JSONArray partitions = details.getJSONObject(d).getJSONArray("ownedPartitions");
                    assertNotNull("每个节点都应带 ownedPartitions", partitions);
                    for (int p = 0; p < partitions.size(); p++) {
                        owned.add(partitions.getString(p));
                    }
                }
                assertTrue("分区认领并集应完整: " + owned,
                        owned.containsAll(Arrays.asList(
                                "public/DEFAULT_GROUP", "public/G0", "public/G1")));
                assertEquals("分区不应被重复认领: " + owned, 3, owned.size());
                // 基础 nodes 列表保留（回退路径）
                assertEquals(3, JSON.parseObject(get(port, "/v1/cs/cluster").body)
                        .getJSONObject("data").getJSONArray("nodes").size());
            }
        } finally {
            stopAll(nodes);
        }
    }

    @Test
    public void shouldSurviveRollingRestart() throws Exception {
        Path[] dataDirs = dataDirs("roll");
        PolyConfigServer[] nodes = new PolyConfigServer[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = startNode(i, dataDirs[i]);
        }
        try {
            DistroAssignments assignments = assignments();
            int ownerIndex = indexOf(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            int otherA = (ownerIndex + 1) % 3;
            int otherB = (ownerIndex + 2) % 3;

            assertEquals(200, post(HTTP_PORTS[otherA], "/v1/cs/configs",
                    publishBody("app.yml", "v1", null)).status);
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("初始 v1 应全员收敛", 8000L, () ->
                        read(port, "app.yml").contains("v1"));
            }

            // 滚 1：停普通副本 A，owner 侧继续写，A 重启后追平
            nodes[otherA].stop();
            assertEquals(200, post(HTTP_PORTS[ownerIndex], "/v1/cs/configs",
                    publishBody("app.yml", "v2", 1L)).status);
            nodes[otherA] = startNode(otherA, dataDirs[otherA]);
            waitUntil("重启后的 A 应追平 v2", 8000L, () ->
                    read(HTTP_PORTS[otherA], "app.yml").contains("v2"));

            // 滚 2：停普通副本 B，同上
            nodes[otherB].stop();
            assertEquals(200, post(HTTP_PORTS[ownerIndex], "/v1/cs/configs",
                    publishBody("app.yml", "v3", 2L)).status);
            nodes[otherB] = startNode(otherB, dataDirs[otherB]);
            waitUntil("重启后的 B 应追平 v3", 8000L, () ->
                    read(HTTP_PORTS[otherB], "app.yml").contains("v3"));

            // 滚 3：停 owner → 该分区写 503、读保持最后同步点；重启后写恢复
            nodes[ownerIndex].stop();
            Response rejected = post(HTTP_PORTS[otherA], "/v1/cs/configs",
                    publishBody("app.yml", "v4", 3L));
            assertEquals(503, rejected.status);
            assertTrue("owner 宕机期间读应保持最后同步点",
                    read(HTTP_PORTS[otherA], "app.yml").contains("v3"));
            nodes[ownerIndex] = startNode(ownerIndex, dataDirs[ownerIndex]);
            assertEquals(200, post(HTTP_PORTS[otherA], "/v1/cs/configs",
                    publishBody("app.yml", "v5", 3L)).status);
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("滚动重启后 v5 应全员收敛", 8000L, () ->
                        read(port, "app.yml").contains("v5"));
            }
        } finally {
            stopAll(nodes);
        }
    }

    @Test
    public void shouldEnforceOptimisticLockUnderCrossNodeConcurrency() throws Exception {
        PolyConfigServer[] nodes = startAll("lock");
        try {
            assertEquals(200, post(HTTP_PORTS[0], "/v1/cs/configs",
                    publishBody("app.yml", "base", null)).status);

            // 三个节点同时以 expectedVersion=1 写入：owner 串行校验，恰好一个成功
            CountDownLatch startGate = new CountDownLatch(1);
            ConcurrentLinkedQueue<Integer> statuses = new ConcurrentLinkedQueue<>();
            List<Thread> writers = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                final String content = "from-node-" + (i + 1);
                Thread writer = new Thread(() -> {
                    try {
                        startGate.await();
                        Response response = post(port, "/v1/cs/configs",
                                publishBody("app.yml", content, 1L));
                        statuses.add(response.status);
                    } catch (Exception ignored) {
                        // 状态码收集线程：异常按未成功计（下面断言会失败）
                    }
                }, "lock-writer-" + (i + 1));
                writer.start();
                writers.add(writer);
            }
            startGate.countDown();
            for (Thread writer : writers) {
                writer.join(10000L);
            }

            int ok = 0;
            int conflict = 0;
            for (Integer status : statuses) {
                if (status == 200) {
                    ok++;
                } else if (status == 409) {
                    conflict++;
                }
            }
            assertEquals("状态码应全部收集: " + statuses, 3, statuses.size());
            assertEquals("并发写应恰好一个成功: " + statuses, 1, ok);
            assertEquals("其余应全部版本冲突: " + statuses, 2, conflict);

            // 胜者内容未知：以任一节点终值为基准，三节点应收敛到同一内容
            String finalValue = read(HTTP_PORTS[0], "app.yml");
            assertTrue("最终值应是某个写入者的内容: " + finalValue,
                    finalValue.contains("from-node-"));
            for (int i = 1; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 应收敛到胜者内容", 8000L, () ->
                        finalValue.equals(read(port, "app.yml")));
            }
        } finally {
            stopAll(nodes);
        }
    }

    private static DistroAssignments assignments() {
        return new DistroAssignments(Arrays.asList(
                new ClusterMember("node-1", "127.0.0.1", 18856, 19856),
                new ClusterMember("node-2", "127.0.0.1", 18857, 19857),
                new ClusterMember("node-3", "127.0.0.1", 18858, 19858)));
    }

    private static int indexOf(String nodeId) {
        for (int i = 0; i < IDS.length; i++) {
            if (IDS[i].equals(nodeId)) {
                return i;
            }
        }
        throw new IllegalArgumentException("未知节点 " + nodeId);
    }

    private Path[] dataDirs(String prefix) throws IOException {
        Path[] dirs = new Path[3];
        for (int i = 0; i < 3; i++) {
            dirs[i] = folder.newFolder(prefix + "-" + IDS[i]).toPath();
        }
        return dirs;
    }

    private PolyConfigServer[] startAll(String prefix) throws IOException {
        Path[] dirs = dataDirs(prefix);
        PolyConfigServer[] nodes = new PolyConfigServer[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = startNode(i, dirs[i]);
        }
        return nodes;
    }

    private static void stopAll(PolyConfigServer[] nodes) {
        for (PolyConfigServer node : nodes) {
            if (node != null) {
                node.stop();
            }
        }
    }

    private static PolyConfigServer startNode(int index, Path dataDir) throws IOException {
        PolyConfigServer server = new PolyConfigServer(ServerOptions.builder()
                .host("127.0.0.1")
                .advertiseHost("127.0.0.1")
                .httpPort(HTTP_PORTS[index])
                .remotingPort(REMOTING_PORTS[index])
                .dataDir(dataDir)
                .nodeId(IDS[index])
                .clusterId("ap-matrix-test")
                .mode("ap")
                .clusterMembers(MEMBERS)
                .verifyIntervalMillis(1000L)
                .build());
        server.start();
        return server;
    }

    private static JSONArray detailsOf(Response cluster) {
        JSONObject data = JSON.parseObject(cluster.body).getJSONObject("data");
        return data == null ? null : data.getJSONArray("details");
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

    private static String publishBody(String dataId, String content, Long expectedVersion) {
        return publishBodyInGroup("DEFAULT_GROUP", dataId, content, expectedVersion);
    }

    private static String publishBodyInGroup(String group, String dataId, String content) {
        return publishBodyInGroup(group, dataId, content, null);
    }

    private static String publishBodyInGroup(String group, String dataId, String content,
                                             Long expectedVersion) {
        JSONObject request = new JSONObject();
        request.put("namespace", "public");
        request.put("group", group);
        request.put("dataId", dataId);
        request.put("content", content);
        request.put("type", "YAML");
        request.put("operator", "tester");
        if (expectedVersion != null) {
            request.put("expectedVersion", expectedVersion);
        }
        return request.toJSONString();
    }

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
        connection.setReadTimeout(8000);
        return read(connection);
    }

    private static Response post(int port, String path, String body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(8000);
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

    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
