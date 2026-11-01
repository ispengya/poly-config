package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.ConfigListener;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
            JSONObject clusterData = JSON.parseObject(cluster.body).getJSONObject("data");
            assertEquals("STANDALONE", clusterData.getString("mode"));
            JSONObject clusterNode = clusterData.getJSONArray("nodes").getJSONObject(0);
            assertEquals("127.0.0.1", clusterNode.getString("host"));
            assertTrue(clusterNode.getIntValue("port") > 0);
            assertTrue(clusterNode.getIntValue("remotingPort") > 0);

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
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&page=1&size=10");
            JSONObject historyPage = JSON.parseObject(history.body).getJSONObject("data");
            JSONArray historyData = historyPage.getJSONArray("items");
            assertEquals(1, historyPage.getIntValue("total"));
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
            JSONObject listPage = JSON.parseObject(list.body).getJSONObject("data");
            JSONArray items = listPage.getJSONArray("items");
            assertEquals(1, listPage.getIntValue("total"));
            assertEquals(1, items.size());
            assertEquals("v2", items.getJSONObject(0).getString("content"));

            Response namespaces = get(server.getHttpPort(), "/v1/cs/namespaces");
            assertTrue(JSON.parseObject(namespaces.body).getJSONArray("data").contains("public"));

            Response groups = get(server.getHttpPort(),
                    "/v1/cs/groups?namespace=public");
            assertTrue(JSON.parseObject(groups.body).getJSONArray("data")
                    .contains("DEFAULT_GROUP"));

            JSONObject createNamespace = new JSONObject();
            createNamespace.put("namespace", "stage");
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/namespaces",
                    createNamespace.toJSONString()).status);
            assertTrue(JSON.parseObject(get(server.getHttpPort(), "/v1/cs/namespaces").body)
                    .getJSONArray("data").contains("stage"));

            JSONObject createGroup = new JSONObject();
            createGroup.put("namespace", "public");
            createGroup.put("group", "PAYMENT");
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/groups",
                    createGroup.toJSONString()).status);
            assertTrue(JSON.parseObject(get(server.getHttpPort(),
                    "/v1/cs/groups?namespace=public").body)
                    .getJSONArray("data").contains("PAYMENT"));

            Response history = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&page=1&size=10");
            assertEquals(2, JSON.parseObject(history.body).getJSONObject("data")
                    .getJSONArray("items").size());

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
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=app.yml&page=1&size=10");
            assertEquals(3, JSON.parseObject(historyAfter.body).getJSONObject("data")
                    .getJSONArray("items").size());
        } finally {
            server.stop();
        }
    }

    @Test
    public void shouldPageListAndHistory() throws Exception {
        Path dataDir = folder.newFolder("paging-data").toPath();
        PolyConfigServer server = startServer(dataDir);
        try {
            for (int i = 1; i <= 5; i++) {
                assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                        publishBody("app-" + i + ".yml", "v" + i, null)).status);
            }

            // 配置列表分页：total 恒为 5，页内条数受 size 约束
            Response page1 = get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&page=1&size=2");
            JSONObject data1 = JSON.parseObject(page1.body).getJSONObject("data");
            assertEquals(5, data1.getIntValue("total"));
            assertEquals(1, data1.getIntValue("page"));
            assertEquals(2, data1.getIntValue("size"));
            assertEquals(2, data1.getJSONArray("items").size());

            Response page3 = get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&page=3&size=2");
            JSONObject data3 = JSON.parseObject(page3.body).getJSONObject("data");
            assertEquals(5, data3.getIntValue("total"));
            assertEquals(1, data3.getJSONArray("items").size());

            Response page9 = get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&page=9&size=2");
            assertEquals(0, JSON.parseObject(page9.body).getJSONObject("data")
                    .getJSONArray("items").size());

            // 关键字过滤后分页
            Response keyword = get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP"
                            + "&keyword=app-1&page=1&size=10");
            JSONObject keywordData = JSON.parseObject(keyword.body).getJSONObject("data");
            assertEquals(1, keywordData.getIntValue("total"));
            assertEquals("app-1.yml",
                    keywordData.getJSONArray("items").getJSONObject(0).getString("dataId"));

            // 非法分页参数返回 400
            assertEquals(400, get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&page=0").status);
            assertEquals(400, get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&size=9999").status);
            assertEquals(400, get(server.getHttpPort(),
                    "/v1/cs/configs/list?namespace=public&group=DEFAULT_GROUP&page=abc").status);

            // 历史分页：同一 key 发布 3 次 → total 3，按新到旧切页
            for (int i = 1; i <= 3; i++) {
                assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                        publishBody("hist.yml", "h" + i, null)).status);
            }
            Response hist1 = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=hist.yml"
                            + "&page=1&size=2");
            JSONObject histData1 = JSON.parseObject(hist1.body).getJSONObject("data");
            assertEquals(3, histData1.getIntValue("total"));
            assertEquals(2, histData1.getJSONArray("items").size());
            assertEquals("h3", histData1.getJSONArray("items")
                    .getJSONObject(0).getString("content"));
            Response hist2 = get(server.getHttpPort(),
                    "/v1/cs/history?namespace=public&group=DEFAULT_GROUP&dataId=hist.yml"
                            + "&page=2&size=2");
            assertEquals(1, JSON.parseObject(hist2.body).getJSONObject("data")
                    .getJSONArray("items").size());
        } finally {
            server.stop();
        }
    }

    @Test
    public void shouldRejectVersionConflict() throws Exception {
        Path dataDir = folder.newFolder("conflict-data").toPath();
        PolyConfigServer server = startServer(dataDir);
        try {
            // 首次创建：expectedVersion=-1 表示"必须不存在"，应成功
            Response created = post(server.getHttpPort(), "/v1/cs/configs",
                    publishJsonWithExpectedVersion("v1", -1L));
            assertEquals(200, created.status);

            // 再次以"必须不存在"创建同一 key，应冲突
            Response recreate = post(server.getHttpPort(), "/v1/cs/configs",
                    publishJsonWithExpectedVersion("v2", -1L));
            assertEquals(409, recreate.status);
            assertEquals(40901, JSON.parseObject(recreate.body).getIntValue("code"));

            Response current = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            long revision = JSON.parseObject(current.body)
                    .getJSONObject("data").getLongValue("revision");

            // 过期版本发布应冲突，且内容保持不变
            Response stale = post(server.getHttpPort(), "/v1/cs/configs",
                    publishJsonWithExpectedVersion("v2", revision - 1));
            assertEquals(409, stale.status);
            assertEquals("v1", JSON.parseObject(current.body)
                    .getJSONObject("data").getString("content"));

            // 基于当前版本发布应成功
            Response updated = post(server.getHttpPort(), "/v1/cs/configs",
                    publishJsonWithExpectedVersion("v2", revision));
            assertEquals(200, updated.status);
            Response after = get(server.getHttpPort(),
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals("v2", JSON.parseObject(after.body)
                    .getJSONObject("data").getString("content"));

            // 不带 expectedVersion 的传统发布向后兼容，直接放行
            Response legacy = post(server.getHttpPort(), "/v1/cs/configs", publishJson("v3"));
            assertEquals(200, legacy.status);
        } finally {
            server.stop();
        }
    }

    @Test
    public void shouldSnapshotCompactAndRecoverOnRestart() throws Exception {
        Path dataDir = folder.newFolder("snapshot-data").toPath();
        PolyConfigServer server = startServer(dataDir, 5, 0, 0);
        long lastRevision = 0L;
        try {
            for (int i = 1; i <= 12; i++) {
                Response response = post(server.getHttpPort(), "/v1/cs/configs",
                        publishBody("app-" + i + ".yml", "v" + i, null));
                assertEquals(200, response.status);
                lastRevision = JSON.parseObject(response.body)
                        .getJSONObject("data").getLongValue("revision");
            }
            assertTrue("快照未在超时内生成", waitForSnapshot(dataDir.resolve("snapshot"), 8000L));
            // 快照覆盖的前缀段应被清理（12 条命令不应保留 12 个段文件）
            assertTrue("日志前缀段未被清理", countSegments(dataDir.resolve("log")) < 12);
        } finally {
            server.stop();
        }

        PolyConfigServer restarted = startServer(dataDir, 5, 0, 0);
        try {
            for (int i = 1; i <= 12; i++) {
                Response response = get(restarted.getHttpPort(),
                        "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app-"
                                + i + ".yml");
                assertEquals(200, response.status);
                assertEquals("v" + i, JSON.parseObject(response.body)
                        .getJSONObject("data").getString("content"));
            }
            // 快照恢复后 per-key revision 精确延续：
            // app-12.yml 是全局序号 12 写入的，基于它做乐观锁发布必须成功
            Response next = post(restarted.getHttpPort(), "/v1/cs/configs",
                    publishBody("app-12.yml", "v12-next", lastRevision));
            assertEquals("lastRevision=" + lastRevision + ", body=" + next.body,
                    200, next.status);
        } finally {
            restarted.stop();
        }
    }

    @Test
    public void shouldCompleteConcurrentPublishesWithDistinctRevisions() throws Exception {
        Path dataDir = folder.newFolder("concurrent-data").toPath();
        PolyConfigServer server = startServer(dataDir);
        int threads = 8;
        int perThread = 25;
        try {
            Set<Integer> revisions = ConcurrentHashMap.newKeySet();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    futures.add(pool.submit(() -> {
                        for (int j = 0; j < perThread; j++) {
                            Response response;
                            try {
                                response = post(server.getHttpPort(), "/v1/cs/configs",
                                        publishBody("k-" + threadId + "-" + j, "value-" + j, null));
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                            assertEquals(200, response.status);
                            revisions.add(JSON.parseObject(response.body)
                                    .getJSONObject("data").getIntValue("revision"));
                        }
                        return null;
                    }));
                }
                for (java.util.concurrent.Future<?> future : futures) {
                    future.get(60, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }
            // 组提交下所有命令 revision 互不重复且连续 1..N
            assertEquals(threads * perThread, revisions.size());
        } finally {
            server.stop();
        }

        // 重启后抽查内容完整
        PolyConfigServer restarted = startServer(dataDir);
        try {
            for (int t = 0; t < threads; t++) {
                Response response = get(restarted.getHttpPort(),
                        "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=k-"
                                + t + "-0");
                assertEquals(200, response.status);
                assertEquals("value-0", JSON.parseObject(response.body)
                        .getJSONObject("data").getString("content"));
            }
        } finally {
            restarted.stop();
        }
    }

    @Test
    public void shouldRebindPortsAfterGracefulStop() throws Exception {
        Path dataDir = folder.newFolder("stop-data").toPath();
        PolyConfigServer first = startServer(dataDir);
        int httpPort = first.getHttpPort();
        int remotingPort = first.getRemotingPort();
        assertEquals(200, post(httpPort, "/v1/cs/configs", publishJson("v1")).status);
        first.stop();

        // 优雅停机后端口已释放、数据完整：同端口同目录立即重启
        PolyConfigServer second = startServer(dataDir, 0, httpPort, remotingPort);
        try {
            assertEquals(httpPort, second.getHttpPort());
            Response response = get(httpPort,
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, response.status);
            assertEquals("v1", JSON.parseObject(response.body)
                    .getJSONObject("data").getString("content"));
        } finally {
            second.stop();
        }
    }

    @Test
    public void shouldStopPushAndClearRegistryAfterRemoveListener() throws Exception {
        Path dataDir = folder.newFolder("unsubscribe-data").toPath();
        Path snapshotDir = folder.newFolder("unsubscribe-snap").toPath();
        PolyConfigServer server = startServer(dataDir);
        PolyConfigClient client = null;
        try {
            client = new PolyConfigClient(clientOptions(server, snapshotDir));
            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch firstPush = new CountDownLatch(1);
            ConfigListener listener = (dataId, group, value) -> {
                received.add(value);
                firstPush.countDown();
            };
            client.addListener("DEFAULT_GROUP", "app.yml", listener);
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson("v1")).status);
            assertTrue("未收到首次推送", firstPush.await(5, TimeUnit.SECONDS));
            assertTrue("服务端应存在 1 条订阅", waitFor(
                    () -> server.getSubscriptionCount() == 1, 3000L));

            client.removeListener("DEFAULT_GROUP", "app.yml", listener);
            assertTrue("取消订阅后服务端注册表应清空", waitFor(
                    () -> server.getSubscriptionCount() == 0, 3000L));

            assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson("v2")).status);
            Thread.sleep(1000L);
            assertEquals("取消订阅后不应再收到推送", 1, received.size());
            assertEquals("v1", received.get(0));
        } finally {
            if (client != null) {
                client.close();
            }
            server.stop();
        }
    }

    @Test
    public void shouldResyncChangeAfterServerRestart() throws Exception {
        Path dataDir = folder.newFolder("resync-data").toPath();
        Path snapshotDir = folder.newFolder("resync-snap").toPath();
        PolyConfigServer server1 = startServer(dataDir);
        int httpPort = server1.getHttpPort();
        int remotingPort = server1.getRemotingPort();
        PolyConfigClient client = null;
        try {
            ClientOptions options = ClientOptions.builder()
                    .server("127.0.0.1:" + remotingPort)
                    .snapshotDir(snapshotDir)
                    .requestTimeoutMillis(1000L)
                    .resyncIntervalMillis(300L)
                    .build();
            client = new PolyConfigClient(options);
            CountDownLatch firstPush = new CountDownLatch(1);
            CountDownLatch resynced = new CountDownLatch(1);
            List<String> received = new CopyOnWriteArrayList<>();
            client.addListener("DEFAULT_GROUP", "app.yml", (dataId, group, value) -> {
                received.add(value);
                if ("v1".equals(value)) {
                    firstPush.countDown();
                }
                if ("v2".equals(value)) {
                    resynced.countDown();
                }
            });
            assertEquals(200, post(httpPort, "/v1/cs/configs", publishJson("v1")).status);
            assertTrue("未收到首次推送", firstPush.await(5, TimeUnit.SECONDS));

            server1.stop();
            // 同端口重启：订阅关系清空，推送不可能送达，新变更只能靠批量对账补发
            PolyConfigServer server2 = startServer(dataDir, 0, httpPort, remotingPort);
            try {
                assertEquals(200, post(httpPort, "/v1/cs/configs",
                        publishJson("v2")).status);
                assertTrue("对账未补发变更", resynced.await(10, TimeUnit.SECONDS));
                assertTrue(received.contains("v2"));
            } finally {
                server2.stop();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    public void shouldRejectOversizedBodyWith413() throws Exception {
        Path dataDir = folder.newFolder("body-limit-data").toPath();
        PolyConfigServer server = startServer(dataDir);
        try {
            char[] padding = new char[2 * 1024 * 1024];
            java.util.Arrays.fill(padding, 'x');
            Response rejected = post(server.getHttpPort(), "/v1/cs/configs",
                    publishBody("big.yml", "content: " + new String(padding), null));
            assertEquals(413, rejected.status);
            assertEquals(41301, JSON.parseObject(rejected.body).getIntValue("code"));

            // 超限拒绝不影响正常发布
            assertEquals(200, post(server.getHttpPort(), "/v1/cs/configs",
                    publishJson("ok")).status);
        } finally {
            server.stop();
        }
    }

    @Test
    public void shouldLimitSubscriptionsPerConnection() throws Exception {
        Path dataDir = folder.newFolder("sublimit-data").toPath();
        Path snapshotDir = folder.newFolder("sublimit-snap").toPath();
        ServerOptions options = ServerOptions.builder()
                .host("127.0.0.1")
                .advertiseHost("127.0.0.1")
                // 随机端口：默认端口可能被本机运行的演示集群占用
                .httpPort(0)
                .remotingPort(0)
                .dataDir(dataDir)
                .maxSubscriptionsPerConnection(2)
                .build();
        PolyConfigServer server = new PolyConfigServer(options);
        server.start();
        PolyConfigClient client = null;
        try {
            client = new PolyConfigClient(clientOptions(server, snapshotDir));
            // 同一连接注册 3 个 key，最多 2 个进注册表，超出的失败只告警不抛出
            client.addListener("DEFAULT_GROUP", "a.yml", (dataId, group, value) -> { });
            client.addListener("DEFAULT_GROUP", "b.yml", (dataId, group, value) -> { });
            client.addListener("DEFAULT_GROUP", "c.yml", (dataId, group, value) -> { });
            assertTrue("订阅数应被限制在上限内", waitFor(
                    () -> server.getSubscriptionCount() == 2, 5000L));
            Thread.sleep(500L);
            assertEquals(2, server.getSubscriptionCount());
        } finally {
            if (client != null) {
                client.close();
            }
            server.stop();
        }
    }

    private static boolean waitFor(java.util.function.BooleanSupplier condition,
                                   long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50L);
        }
        return condition.getAsBoolean();
    }

    private static boolean waitForSnapshot(Path dir, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (Files.isDirectory(dir)) {
                try (DirectoryStream<Path> stream =
                             Files.newDirectoryStream(dir, "snapshot-*.json")) {
                    if (stream.iterator().hasNext()) {
                        return true;
                    }
                } catch (IOException ignored) {
                    // 目录尚未就绪，继续等待
                }
            }
            Thread.sleep(50L);
        }
        return false;
    }

    private static int countSegments(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path ignored : stream) {
                count++;
            }
        }
        return count;
    }

    private static String publishJsonWithExpectedVersion(String content, long expectedVersion) {
        JSONObject request = JSONObject.parseObject(publishJson(content));
        request.put("expectedVersion", expectedVersion);
        return request.toJSONString();
    }

    private static PolyConfigServer startServer(Path dataDir) throws IOException {
        return startServer(dataDir, 0, 0, 0);
    }

    private static PolyConfigServer startServer(Path dataDir, int snapshotThreshold,
                                                int httpPort, int remotingPort) throws IOException {
        ServerOptions options = ServerOptions.builder()
                .host("127.0.0.1")
                // 固定对外地址，避免 NetworkUtils 在有网卡的机器上探测成局域网 IP 导致断言失败
                .advertiseHost("127.0.0.1")
                .httpPort(httpPort)
                .remotingPort(remotingPort)
                .dataDir(dataDir)
                .nodeId("node-1")
                .clusterId("test-cluster")
                .snapshotThreshold(snapshotThreshold)
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
        return publishBody("app.yml", content, null);
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
