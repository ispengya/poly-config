package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 单机核心链路端到端测试。
 */
public class CoreChainIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldPushPersistAndFallbackToSnapshot() throws Exception {
        Path dataDir = folder.newFolder("data").toPath();
        Path snapshotDir = folder.newFolder("snapshots").toPath();
        String content = "server:\n  port: 8080\n";
        PolyConfigServer server = startServer(dataDir);
        PolyConfigClient client = null;
        try {
            Response console = get(server.getHttpPort(), "/");
            assertEquals(200, console.status);
            assertTrue(console.body.contains("poly-config"));

            Response cluster = get(server.getHttpPort(), "/v1/cs/cluster");
            assertEquals(200, cluster.status);
            assertEquals("STANDALONE",
                    JSON.parseObject(cluster.body).getJSONObject("data").getString("mode"));

            client = new PolyConfigClient(clientOptions(server, snapshotDir));
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<String> received = new AtomicReference<>();
            client.addListener("DEFAULT_GROUP", "app.yml",
                    (dataId, group, value) -> {
                        received.set(value);
                        latch.countDown();
                    });

            Response publish = post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson(content));
            assertEquals(200, publish.status);
            assertEquals(0, JSON.parseObject(publish.body).getIntValue("code"));

            assertTrue("未收到配置推送", latch.await(5, TimeUnit.SECONDS));
            assertEquals(content, received.get());
            assertEquals(content, client.getConfig("DEFAULT_GROUP", "app.yml", 2000));

            Response history = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&limit=10");
            JSONArray historyData = JSON.parseObject(history.body).getJSONArray("data");
            assertEquals(1, historyData.size());
            assertEquals("PUBLISH", historyData.getJSONObject(0).getString("operation"));
        } finally {
            if (client != null) {
                client.close();
            }
            server.stop();
        }

        PolyConfigClient offline = new PolyConfigClient(clientOptions(server, snapshotDir));
        try {
            assertEquals(content, offline.getConfig("DEFAULT_GROUP", "app.yml", 500));
        } finally {
            offline.close();
        }

        PolyConfigServer restarted = startServer(dataDir);
        try {
            Response get = get(restarted.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, get.status);
            JSONObject data = JSON.parseObject(get.body).getJSONObject("data");
            assertEquals(content, data.getString("content"));
        } finally {
            restarted.stop();
        }
    }

    @Test
    public void shouldListAndRollback() throws Exception {
        Path dataDir = folder.newFolder("rollback-data").toPath();
        PolyConfigServer server = startServer(dataDir);
        try {
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson("v1")).status);
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson("v2")).status);

            Response list = get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP");
            JSONArray items = JSON.parseObject(list.body).getJSONArray("data");
            assertEquals(1, items.size());
            assertEquals("v2", items.getJSONObject(0).getString("content"));

            Response history = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&limit=10");
            assertEquals(2, JSON.parseObject(history.body).getJSONArray("data").size());

            JSONObject rollback = new JSONObject();
            rollback.put("namespace", "public");
            rollback.put("group", "DEFAULT_GROUP");
            rollback.put("dataId", "app.yml");
            rollback.put("revision", 1L);
            rollback.put("operator", "tester");
            Response rollbackResponse = post(server.getHttpPort(),
                    "/v1/cs/history/rollback", rollback.toJSONString());
            assertEquals(200, rollbackResponse.status);
            assertEquals(0, JSON.parseObject(rollbackResponse.body).getIntValue("code"));

            Response current = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals("v1", JSON.parseObject(current.body)
                    .getJSONObject("data").getString("content"));

            Response historyAfter = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&limit=10");
            assertEquals(3, JSON.parseObject(historyAfter.body).getJSONArray("data").size());
        } finally {
            server.stop();
        }
    }

    private static PolyConfigServer startServer(Path dataDir) throws IOException {
        ServerOptions options = ServerOptions.builder()
                .host("127.0.0.1")
                .httpPort(0)
                .remotingPort(0)
                .dataDir(dataDir)
                .nodeId("node-1")
                .clusterId("test-cluster")
                .build();
        PolyConfigServer server = new PolyConfigServer(options);
        server.start();
        return server;
    }

    private static ClientOptions clientOptions(PolyConfigServer server, Path snapshotDir) {
        return ClientOptions.builder()
                .server("127.0.0.1:" + server.getRemotingPort())
                .snapshotDir(snapshotDir)
                .requestTimeoutMillis(1000L)
                .resyncIntervalMillis(60000L)
                .build();
    }

    private static String publishJson(String content) {
        JSONObject request = new JSONObject();
        request.put("namespace", "public");
        request.put("group", "DEFAULT_GROUP");
        request.put("dataId", "app.yml");
        request.put("content", content);
        request.put("type", "YAML");
        request.put("operator", "tester");
        return request.toJSONString();
    }

    private static Response get(int port, String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("GET");
        return read(connection);
    }

    private static Response post(int port, String path, String body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        connection.setFixedLengthStreamingMode(bytes.length);
        try (OutputStream output = connection.getOutputStream()) {
            output.write(bytes);
        }
        return read(connection);
    }

    private static Response read(HttpURLConnection connection) throws IOException {
        int status = connection.getResponseCode();
        InputStream input = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        String body = "";
        if (input != null) {
            try {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    output.write(buffer, 0, read);
                }
                body = new String(output.toByteArray(), StandardCharsets.UTF_8);
            } finally {
                input.close();
            }
        }
        connection.disconnect();
        return new Response(status, body);
    }

    /**
     * HTTP 响应。
     */
    private static final class Response {

        /**
         * HTTP 状态码。
         */
        private final int status;

        /**
         * 响应体。
         */
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
