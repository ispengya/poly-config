package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
import com.ispengya.polyconfig.common.error.PolyConfigException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AP-1 端到端测试：mode=ap 的单节点以静态成员视图运行。
 */
public class ApClusterIntegrationTest {

    // 隔离端口段，避免成员探测打到本机上正在运行的演示集群
    private static final String MEMBERS =
            "node-1=127.0.0.1:18848:19848,"
                    + "node-2=127.0.0.1:18849:19849,"
                    + "node-3=127.0.0.1:18850:19850";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldServeAsSingleNodeInApCluster() throws Exception {
        Path dataDir = folder.newFolder("ap-cluster").toPath();
        PolyConfigServer server = startApServer(dataDir);
        try {
            Response cluster = get(server.getHttpPort(), "/v1/cs/cluster");
            assertEquals(200, cluster.status);
            JSONObject clusterData = JSON.parseObject(cluster.body).getJSONObject("data");
            assertEquals("AP", clusterData.getString("mode"));
            assertEquals("node-1", clusterData.getString("currentNodeId"));
            assertEquals(3, clusterData.getJSONArray("nodes").size());
            JSONObject self = clusterData.getJSONArray("nodes").getJSONObject(0);
            assertEquals("node-1", self.getString("nodeId"));
            assertEquals("PEER", self.getString("role"));
            assertTrue(self.getBooleanValue("healthy"));

            Response publish = post(server.getHttpPort(), "/v1/cs/configs",
                    publishBody("app.yml", "v1", null));
            assertEquals(200, publish.status);
            assertEquals(0, JSON.parseObject(publish.body).getIntValue("code"));

            Response read = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals("v1", JSON.parseObject(read.body)
                    .getJSONObject("data").getString("content"));

            // AP 模式下乐观锁语义保持：-1 表示必须不存在，第二次发布应 409
            Response conflict = post(server.getHttpPort(), "/v1/cs/configs",
                    publishBody("app.yml", "v2", -1L));
            assertEquals(409, conflict.status);
        } finally {
            server.stop();
        }

        // 重启后数据仍在（AP 节点从本地日志+快照恢复）
        PolyConfigServer restarted = startApServer(dataDir);
        try {
            Response read = get(restarted.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, read.status);
            assertEquals("v1", JSON.parseObject(read.body)
                    .getJSONObject("data").getString("content"));
        } finally {
            restarted.stop();
        }
    }

    @Test
    public void shouldExposeSubscribersViaApi() throws Exception {
        Path dataDir = folder.newFolder("ap-subs").toPath();
        Path snapshotDir = folder.newFolder("ap-subs-snap").toPath();
        PolyConfigServer server = startApServer(dataDir);
        PolyConfigClient client = null;
        try {
            Response empty = get(server.getHttpPort(), "/v1/cs/subscriptions");
            assertEquals(200, empty.status);
            JSONObject emptyData = JSON.parseObject(empty.body).getJSONObject("data");
            assertEquals(0, emptyData.getIntValue("total"));
            assertEquals(0, emptyData.getJSONArray("connections").size());

            client = new PolyConfigClient(ClientOptions.builder()
                    .server("127.0.0.1:" + server.getRemotingPort())
                    .snapshotDir(snapshotDir)
                    .requestTimeoutMillis(1000L)
                    .resyncIntervalMillis(60000L)
                    .build());
            client.addListener("DEFAULT_GROUP", "app.yml", (dataId, group, value) -> { });

            JSONObject data = null;
            long deadline = System.currentTimeMillis() + 5000L;
            while (System.currentTimeMillis() < deadline) {
                Response response = get(server.getHttpPort(), "/v1/cs/subscriptions");
                data = JSON.parseObject(response.body).getJSONObject("data");
                if (data.getIntValue("total") >= 1) {
                    break;
                }
                Thread.sleep(100L);
            }
            assertTrue("订阅应出现在观测接口", data != null && data.getIntValue("total") >= 1);
            JSONArray connections = data.getJSONArray("connections");
            assertTrue(connections.size() >= 1);
            JSONObject first = connections.getJSONObject(0);
            assertTrue(first.getIntValue("count") >= 1);
            assertTrue("订阅键应为 namespace/group/dataId 形式: " + first,
                    first.getJSONArray("keys").contains("public/DEFAULT_GROUP/app.yml"));
        } finally {
            if (client != null) {
                client.close();
            }
            server.stop();
        }
    }

    @Test
    public void shouldRejectCpModeAsUnimplemented() throws Exception {
        Path dataDir = folder.newFolder("cp-data").toPath();
        try {
            new PolyConfigServer(ServerOptions.builder()
                    .host("127.0.0.1")
                    .dataDir(dataDir)
                    .mode("cp")
                    .build());
            fail("CP 模式应明确报未实现");
        } catch (PolyConfigException expected) {
            assertTrue(expected.getMessage().contains("CP"));
        }
    }

    private static PolyConfigServer startApServer(Path dataDir) throws IOException {
        PolyConfigServer server = new PolyConfigServer(ServerOptions.builder()
                .host("127.0.0.1")
                .advertiseHost("127.0.0.1")
                .httpPort(0)
                .remotingPort(0)
                .dataDir(dataDir)
                .nodeId("node-1")
                .clusterId("ap-test")
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
