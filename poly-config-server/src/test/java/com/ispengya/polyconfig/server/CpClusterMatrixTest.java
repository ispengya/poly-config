package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CP 三节点测试矩阵（docs/cp-design.md §11 CP-5 验收）：
 * 全链路写任意节点收敛+推送、RAFT_STATUS 状态采集聚合、杀 leader 换代
 * 与写读语义、跨节点并发乐观锁、SDK ReadConsistency（D8）。
 *
 * <p>端口段 18876-18878 / 19876-19878，与 AP 矩阵（18856-58）及
 * CP 集成测试（18866-68）隔离；选举超时调小（300ms/50ms）。</p>
 */
public class CpClusterMatrixTest {

    private static final String[] IDS = {"node-1", "node-2", "node-3"};
    private static final int[] HTTP_PORTS = {18876, 18877, 18878};
    private static final int[] REMOTING_PORTS = {19876, 19877, 19878};
    private static final String MEMBERS =
            "node-1=127.0.0.1:18876:19876,node-2=127.0.0.1:18877:19877,"
                    + "node-3=127.0.0.1:18878:19878";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldConvergeAndPushAcrossThreeNodes() throws Exception {
        PolyConfigServer[] nodes = startAll("push");
        List<PolyConfigClient> clients = new ArrayList<>();
        try {
            // 客户端各连一个节点：分别覆盖 leader 本机、转发节点与第三节点的推送
            CountDownLatch[] latches = new CountDownLatch[3];
            AtomicReference<String>[] pushed = new AtomicReference[3];
            for (int i = 0; i < 3; i++) {
                latches[i] = new CountDownLatch(1);
                pushed[i] = new AtomicReference<>();
                PolyConfigClient client = newPolyClient(i, "push-snap-" + i);
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
                waitUntil("node-" + (i + 1) + " 应收到订阅", 8000L, () -> {
                    Response subs = get(port, "/v1/cs/subscriptions");
                    return subs.status == 200
                            && JSON.parseObject(subs.body).getJSONObject("data")
                            .getIntValue("total") >= 1;
                });
            }

            // 写非 leader 节点：RAFT_FORWARD → leader 提交 → apply 即推送全体
            LeaderInfo leader = awaitExactlyOneLeader();
            int writerIndex = (leader.index + 1) % 3;
            Response publish = post(HTTP_PORTS[writerIndex], "/v1/cs/configs",
                    publishBody("app.yml", "matrix-v1", null));
            assertEquals(200, publish.status);

            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 应经复制收敛", 8000L, () ->
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
            LeaderInfo leader = awaitExactlyOneLeader();
            Response publish = post(HTTP_PORTS[0], "/v1/cs/configs",
                    publishBody("app.yml", "status-v1", null));
            assertEquals(200, publish.status);
            // no-op=1 + 业务写=2
            long writtenIndex = 2L;

            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 的状态采集应含对端复制位点", 10000L, () -> {
                    JSONArray details = detailsOf(get(port, "/v1/cs/cluster"));
                    if (details == null || details.size() != 3) {
                        return false;
                    }
                    int leaders = 0;
                    JSONObject leaderDetail = null;
                    for (int d = 0; d < details.size(); d++) {
                        JSONObject detail = details.getJSONObject(d);
                        if ("LEADER".equals(detail.getString("role"))) {
                            leaders++;
                            leaderDetail = detail;
                        }
                        if (detail.getLongValue("term") <= 0L
                                || detail.getLongValue("commitIndex") <= 0L) {
                            return false;
                        }
                    }
                    if (leaders != 1 || leaderDetail == null) {
                        return false;
                    }
                    // leader 自报的 peer matchIndex 应覆盖已写位点
                    JSONArray peers = leaderDetail.getJSONArray("peers");
                    if (peers == null || peers.size() != 2) {
                        return false;
                    }
                    for (int p = 0; p < peers.size(); p++) {
                        if (peers.getJSONObject(p).getLongValue("matchIndex")
                                < writtenIndex) {
                            return false;
                        }
                    }
                    return leaderDetail.getLongValue("commitIndex") >= writtenIndex;
                });

                JSONArray details = detailsOf(get(port, "/v1/cs/cluster"));
                assertNotNull(details);
                assertEquals(3, details.size());

                // nodes 视图（回退路径保留）：恰好一个真实 LEADER，任期非零
                JSONObject data = JSON.parseObject(
                        get(port, "/v1/cs/cluster").body).getJSONObject("data");
                JSONArray nodeArray = data.getJSONArray("nodes");
                assertEquals(3, nodeArray.size());
                int nodeLeaders = 0;
                for (int n = 0; n < nodeArray.size(); n++) {
                    JSONObject node = nodeArray.getJSONObject(n);
                    if ("LEADER".equals(node.getString("role"))) {
                        nodeLeaders++;
                    }
                    assertTrue("自报任期应 ≥1: " + node,
                            node.getLongValue("term") >= 1L);
                }
                assertEquals("nodes 视图应恰好一个 leader", 1, nodeLeaders);
            }
        } finally {
            stopAll(nodes);
        }
    }

    @Test
    public void shouldReelectAndKeepWriteReadSemanticsAfterLeaderKill() throws Exception {
        Path[] dataDirs = dataDirs("reelect");
        PolyConfigServer[] nodes = new PolyConfigServer[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = startNode(i, dataDirs[i]);
        }
        try {
            LeaderInfo leader = awaitExactlyOneLeader();
            ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "reelect.yml");

            assertEquals(200, post(HTTP_PORTS[leader.index], "/v1/cs/configs",
                    publishBody("reelect.yml", "v1", null)).status);
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("初始 v1 应全员收敛", 8000L, () ->
                        read(port, "reelect.yml").contains("v1"));
            }
            assertEquals("v1", linearizable(nodes[leader.index], key));

            // 杀 leader 的同时从存活节点写：stale leaderId 转发到已死节点/协议
            // 已关 → 503 带 leader 提示（写失败而非静默丢弃）
            int survivorA = (leader.index + 1) % 3;
            int survivorB = (leader.index + 2) % 3;
            AtomicReference<Response> duringElection = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    // 等协议关闭、选举尚未发生（随机超时 ≥300ms）的窗口；
                    // expectedVersion=2 对准首写 v1 的 version（no-op 占 1 号）
                    Thread.sleep(100L);
                    duringElection.set(post(HTTP_PORTS[survivorA], "/v1/cs/configs",
                            publishBody("reelect.yml", "v2", 2L)));
                } catch (Exception ignored) {
                    // 无响应按失败计（下面断言会失败）
                }
            }, "cp-reelect-writer");
            writer.start();
            killServer(nodes[leader.index]);
            writer.join(10000L);
            assertNotNull("选举窗口写应有响应", duringElection.get());
            assertEquals("选举中写应 503, body=" + duringElection.get().body,
                    503, duringElection.get().status);
            assertTrue("503 应带 leader/无主提示: " + duringElection.get().body,
                    duringElection.get().body.contains("leader")
                            || duringElection.get().body.contains("选举"));

            // 存活节点换代：新 leader 写 v2 → 线性读必须读到 v2（不回退）
            LeaderInfo second = awaitExactlyOneLeader(new int[]{survivorA, survivorB});
            assertTrue("新 leader term 应更高", second.term > leader.term);
            Response republish = post(HTTP_PORTS[second.index], "/v1/cs/configs",
                    publishBody("reelect.yml", "v2", 2L));
            assertEquals("换代后写应恢复, body=" + republish.body, 200, republish.status);
            assertEquals("v2", linearizable(nodes[second.index], key));
            assertEquals("v2", linearizable(nodes[survivorA], key));

            for (int i = 0; i < 3; i++) {
                if (i == leader.index) {
                    continue;
                }
                final int port = HTTP_PORTS[i];
                waitUntil("存活节点应收敛到 v2", 8000L, () ->
                        read(port, "reelect.yml").contains("v2"));
            }
        } finally {
            stopAll(nodes);
        }
    }

    @Test
    public void shouldEnforceOptimisticLockUnderCrossNodeConcurrency() throws Exception {
        PolyConfigServer[] nodes = startAll("lock");
        try {
            // CP 写要等选出 leader（首写之前集群可能还在选举窗口）
            awaitExactlyOneLeader();
            assertEquals(200, post(HTTP_PORTS[0], "/v1/cs/configs",
                    publishBody("app.yml", "base", null)).status);

            // 三个节点同时以 expectedVersion=2 写入（CP：no-op 占 1 号，首写 version=2）：
            // leader 侧 D5 串行校验，恰好一个成功
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
                                publishBody("app.yml", content, 2L));
                        statuses.add(response.status);
                    } catch (Exception ignored) {
                        // 状态码收集线程：异常按未成功计（下面断言会失败）
                    }
                }, "cp-lock-writer-" + (i + 1));
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

            // 胜者内容未知且复制有界延迟：先从任一节点读到胜者值，再等全员收敛
            AtomicReference<String> winner = new AtomicReference<>();
            waitUntil("应有节点读到胜者内容", 8000L, () -> {
                for (int p = 0; p < 3; p++) {
                    String value = read(HTTP_PORTS[p], "app.yml");
                    if (value.contains("from-node-")) {
                        winner.set(value);
                        return true;
                    }
                }
                return false;
            });
            String finalValue = winner.get();
            assertNotNull(finalValue);
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("node-" + (i + 1) + " 应收敛到胜者内容", 8000L, () ->
                        finalValue.equals(read(port, "app.yml")));
            }
        } finally {
            stopAll(nodes);
        }
    }

    @Test
    public void shouldServeSdkLinearizableReadAndFailClosedDuringMajorityLoss()
            throws Exception {
        Path[] dataDirs = dataDirs("sdk");
        PolyConfigServer[] nodes = new PolyConfigServer[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = startNode(i, dataDirs[i]);
        }
        PolyConfigClient client = null;
        try {
            LeaderInfo leader = awaitExactlyOneLeader();
            assertEquals(200, post(HTTP_PORTS[leader.index], "/v1/cs/configs",
                    publishBody("sdk.yml", "sdk-v1", null)).status);
            for (int i = 0; i < 3; i++) {
                final int port = HTTP_PORTS[i];
                waitUntil("初始 sdk-v1 应全员收敛", 8000L, () ->
                        read(port, "sdk.yml").contains("sdk-v1"));
            }

            // D8：SDK 连 leader，LOCAL 与 LINEARIZABLE 都读到 sdk-v1
            client = newPolyClient(leader.index, "sdk-snap");
            assertEquals("sdk-v1", client.getConfig("public", "DEFAULT_GROUP",
                    "sdk.yml", 3000L));
            assertEquals("sdk-v1", client.getConfig("public", "DEFAULT_GROUP",
                    "sdk.yml", 5000L, ReadConsistency.LINEARIZABLE));

            // 杀多数派：LINEARIZABLE 必须失败（UNAVAILABLE）——
            // 即使本地缓存/快照里有 sdk-v1 也不得兜底返回（违背强一致语义）
            for (int i = 0; i < 3; i++) {
                if (i != leader.index) {
                    killServer(nodes[i]);
                }
            }
            try {
                client.getConfig("public", "DEFAULT_GROUP", "sdk.yml",
                        5000L, ReadConsistency.LINEARIZABLE);
                fail("多数派宕机时 SDK 线性读应失败而非返回本地旧值");
            } catch (PolyConfigException e) {
                assertEquals(ErrorCode.UNAVAILABLE, e.getErrorCode());
            }
            // LOCAL 语义对照：本地缓存照常可读（允许陈旧）
            assertEquals("sdk-v1", client.getConfig("public", "DEFAULT_GROUP",
                    "sdk.yml", 3000L));

            // 恢复多数派：线性读恢复可用
            for (int i = 0; i < 3; i++) {
                if (i != leader.index) {
                    nodes[i] = startNode(i, dataDirs[i]);
                }
            }
            awaitExactlyOneLeader();
            assertEquals("sdk-v1", client.getConfig("public", "DEFAULT_GROUP",
                    "sdk.yml", 5000L, ReadConsistency.LINEARIZABLE));
        } finally {
            if (client != null) {
                client.close();
            }
            stopAll(nodes);
        }
    }

    // ==================== 辅助 ====================

    private String linearizable(PolyConfigServer server, ConfigKey key) {
        Optional<ConfigEntry> entry = ((RaftProtocol) server.getProtocol())
                .read(key, ReadConsistency.LINEARIZABLE);
        return entry.map(ConfigEntry::getContent).orElse(null);
    }

    private PolyConfigClient newPolyClient(int nodeIndex, String snapshotName)
            throws IOException {
        return new PolyConfigClient(ClientOptions.builder()
                .server("127.0.0.1:" + REMOTING_PORTS[nodeIndex])
                .snapshotDir(folder.newFolder(snapshotName).toPath())
                .requestTimeoutMillis(5000L)
                .resyncIntervalMillis(60000L)
                .build());
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
                try {
                    node.stop();
                } catch (RuntimeException ignored) {
                    // 已停止的节点重复停止
                }
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
                .clusterId("cp-matrix-test")
                .mode("cp")
                .clusterMembers(MEMBERS)
                .raftElectionTimeoutMillis(300L)
                .raftHeartbeatMillis(50L)
                .build());
        server.start();
        return server;
    }

    /**
     * 杀节点（测试故障注入）：先秒级关协议再优雅停机，避免停机窗口内
     * 参与选举把真 leader 拉下马（与 CpClusterIntegrationTest 同款）。
     */
    private static void killServer(PolyConfigServer server) {
        server.getConfigService().close();
        server.stop();
    }

    private static LeaderInfo awaitExactlyOneLeader() throws Exception {
        int[] all = {0, 1, 2};
        return awaitExactlyOneLeader(all);
    }

    private static LeaderInfo awaitExactlyOneLeader(int[] candidates) throws Exception {
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            // 各候选节点自报真实角色（与 CpClusterIntegrationTest 同款视角约定）：
            // 恰好一个自报 LEADER ⟺ 集群里恰好一个真实 leader
            LeaderInfo found = null;
            int leaderCount = 0;
            for (int i = 0; i < candidates.length; i++) {
                int index = candidates[i];
                Response cluster = get(HTTP_PORTS[index], "/v1/cs/cluster");
                if (cluster.status != 200) {
                    continue;
                }
                JSONObject data = JSON.parseObject(cluster.body).getJSONObject("data");
                JSONArray nodes = data == null ? null : data.getJSONArray("nodes");
                if (nodes == null) {
                    continue;
                }
                for (int n = 0; n < nodes.size(); n++) {
                    JSONObject node = nodes.getJSONObject(n);
                    if (IDS[index].equals(node.getString("nodeId"))
                            && "LEADER".equals(node.getString("role"))) {
                        leaderCount++;
                        found = new LeaderInfo(index, node.getString("nodeId"),
                                node.getLongValue("term"));
                    }
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

    private static final class LeaderInfo {
        private final int index;
        private final String nodeId;
        private final long term;

        private LeaderInfo(int index, String nodeId, long term) {
            this.index = index;
            this.nodeId = nodeId;
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
