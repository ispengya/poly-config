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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * AP 异步复制端到端测试（双节点）：owner 写入后非 owner 收到数据与本地推送，
 * workspace 元数据经内置分区标记复制到全体节点。
 */
public class ApSyncIntegrationTest {

    private static final String MEMBERS =
            "node-1=127.0.0.1:18852:19852,node-2=127.0.0.1:18853:19853";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldReplicateWriteToNonOwnerAndPushLocalSubscriber() throws Exception {
        Path dataDir1 = folder.newFolder("sync-node1").toPath();
        Path dataDir2 = folder.newFolder("sync-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18852, 19852, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18853, 19853, dataDir2);
        PolyConfigClient client = null;
        try {
            DistroAssignments assignments = new DistroAssignments(Arrays.asList(
                    new ClusterMember("node-1", "127.0.0.1", 18852, 19852),
                    new ClusterMember("node-2", "127.0.0.1", 18853, 19853)));
            boolean ownerIsNode1 = "node-1"
                    .equals(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            int ownerHttp = (ownerIsNode1 ? node1 : node2).getHttpPort();
            PolyConfigServer nonOwner = ownerIsNode1 ? node2 : node1;
            int nonOwnerHttp = nonOwner.getHttpPort();

            // 客户端连非 owner：数据与推送都必须由异步复制触发
            client = new PolyConfigClient(ClientOptions.builder()
                    .server("127.0.0.1:" + nonOwner.getRemotingPort())
                    .snapshotDir(folder.newFolder("sync-snap").toPath())
                    .requestTimeoutMillis(1000L)
                    .resyncIntervalMillis(60000L)
                    .build());
            CountDownLatch pushed = new CountDownLatch(1);
            AtomicReference<String> pushedValue = new AtomicReference<>();
            client.addListener("DEFAULT_GROUP", "app.yml", (dataId, group, value) -> {
                pushedValue.set(value);
                pushed.countDown();
            });
            waitUntil("订阅应注册到非 owner", 5000L, () -> {
                Response subs = get(nonOwnerHttp, "/v1/cs/subscriptions");
                return JSON.parseObject(subs.body).getJSONObject("data").getIntValue("total") >= 1;
            });

            // 经 owner 发布：owner 落盘后异步复制到非 owner
            Response publish = post(ownerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v1", null));
            assertEquals(200, publish.status);

            waitUntil("非 owner 应通过复制拿到数据", 8000L, () -> {
                Response read = get(nonOwnerHttp,
                        "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
                return read.status == 200 && "v1".equals(JSON.parseObject(read.body)
                        .getJSONObject("data").getString("content"));
            });
            assertTrue("连非 owner 的客户端应收到复制触发的本地推送",
                    pushed.await(8, TimeUnit.SECONDS));
            assertEquals("v1", pushedValue.get());

            // 复制不改变乐观锁语义：经非 owner 写回 owner，版本仍由 owner 分配
            Response updated = post(nonOwnerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v2", 1L));
            assertEquals(200, updated.status);
            Response stale = post(nonOwnerHttp, "/v1/cs/configs",
                    publishBody("app.yml", "v3", 1L));
            assertEquals(409, stale.status);
        } finally {
            if (client != null) {
                client.close();
            }
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void shouldReplicateWorkspaceMetadataAcrossNodes() throws Exception {
        Path dataDir1 = folder.newFolder("ws-node1").toPath();
        Path dataDir2 = folder.newFolder("ws-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18852, 19852, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18853, 19853, dataDir2);
        try {
            Response createdNs = post(node1.getHttpPort(), "/v1/cs/namespaces",
                    "{\"namespace\":\"tenant-a\"}");
            assertEquals(200, createdNs.status);
            Response createdGroup = post(node2.getHttpPort(), "/v1/cs/groups",
                    "{\"namespace\":\"tenant-a\",\"group\":\"billing\"}");
            assertEquals(200, createdGroup.status);

            for (int httpPort : new int[]{node1.getHttpPort(), node2.getHttpPort()}) {
                final int port = httpPort;
                waitUntil("节点 " + port + " 应看到 tenant-a", 8000L, () -> {
                    Response ns = get(port, "/v1/cs/namespaces");
                    JSONArray list = JSON.parseObject(ns.body).getJSONArray("data");
                    return list != null && list.contains("tenant-a");
                });
                waitUntil("节点 " + port + " 应看到 billing 分组", 8000L, () -> {
                    Response groups = get(port, "/v1/cs/groups?namespace=tenant-a");
                    JSONArray list = JSON.parseObject(groups.body).getJSONArray("data");
                    return list != null && list.contains("billing");
                });
                // 内建系统分区不对外展示
                Response ns = get(port, "/v1/cs/namespaces");
                JSONArray list = JSON.parseObject(ns.body).getJSONArray("data");
                assertNotNull(list);
                assertTrue("_system 不应出现在命名空间列表: " + list,
                        !list.contains("_system"));
            }
        } finally {
            node1.stop();
            node2.stop();
        }
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
                .clusterId("ap-sync-test")
                .mode("ap")
                .clusterMembers(MEMBERS)
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

    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
