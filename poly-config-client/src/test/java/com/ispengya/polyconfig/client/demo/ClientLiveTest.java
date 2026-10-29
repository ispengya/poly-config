package com.ispengya.polyconfig.client.demo;

import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 连接本地运行中的 server 的客户端实测。
 *
 * <p>服务端不可达时自动跳过，不影响普通 mvn test。先启动 standalone server，
 * 再运行：mvn -pl poly-config-client test</p>
 */
public class ClientLiveTest {

    @Test
    public void shouldPublishAndReceivePush() throws Exception {
        String httpBase = System.getProperty("polyconfig.http", "http://127.0.0.1:8848");
        String remoting = System.getProperty("polyconfig.remoting", "127.0.0.1:9848");
        Assume.assumeTrue("服务端不可达，跳过客户端实测: " + httpBase,
                isServerUp(httpBase));

        String group = "DEFAULT_GROUP";
        String dataId = "client-live.yml";
        String first = "server:\n  port: 8080\n# live " + System.currentTimeMillis() + "\n";

        ClientOptions options = ClientOptions.builder()
                .server(remoting)
                .snapshotDir(Paths.get("target", "client-live-snapshot"))
                .requestTimeoutMillis(3000L)
                .resyncIntervalMillis(60000L)
                .build();

        final CountDownLatch firstLatch = new CountDownLatch(1);
        final AtomicReference<String> firstReceived = new AtomicReference<>();
        try (PolyConfigClient client = new PolyConfigClient(options)) {
            client.addListener(group, dataId, (id, g, content) -> {
                firstReceived.set(content);
                firstLatch.countDown();
            });

            publish(httpBase, group, dataId, first);
            assertTrue("第一次变更未收到推送", firstLatch.await(5, TimeUnit.SECONDS));
            assertEquals(first, firstReceived.get());
            assertEquals(first, client.getConfig(group, dataId, 3000L));

            String second = first + "# second\n";
            final CountDownLatch secondLatch = new CountDownLatch(1);
            final AtomicReference<String> secondReceived = new AtomicReference<>();
            client.addListener(group, dataId, (id, g, content) -> {
                secondReceived.set(content);
                secondLatch.countDown();
            });

            publish(httpBase, group, dataId, second);
            assertTrue("第二次变更未收到推送", secondLatch.await(5, TimeUnit.SECONDS));
            assertEquals(second, secondReceived.get());
            assertEquals(second, client.getConfig(group, dataId, 3000L));
        }
    }

    private static boolean isServerUp(String httpBase) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(httpBase + "/v1/cs/health").openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            return connection.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static void publish(String httpBase, String group, String dataId, String content)
            throws IOException {
        JSONObject request = new JSONObject();
        request.put("namespace", "public");
        request.put("group", group);
        request.put("dataId", dataId);
        request.put("content", content);
        request.put("type", "YAML");
        request.put("operator", "client-live-test");

        HttpURLConnection connection = (HttpURLConnection) new URL(
                httpBase + "/v1/cs/configs").openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        byte[] body = request.toJSONString().getBytes(StandardCharsets.UTF_8);
        connection.setFixedLengthStreamingMode(body.length);
        try (OutputStream output = connection.getOutputStream()) {
            output.write(body);
        }
        int status = connection.getResponseCode();
        String response = read(connection, status);
        connection.disconnect();
        if (status != 200) {
            throw new IllegalStateException("发布失败: " + status + " " + response);
        }
    }

    private static String read(HttpURLConnection connection, int status) throws IOException {
        InputStream input = status >= 400
                ? connection.getErrorStream() : connection.getInputStream();
        if (input == null) {
            return "";
        }
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            input.close();
        }
    }
}
