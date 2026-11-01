package com.ispengya.polyconfig.server;

import com.alibaba.fastjson2.JSON;
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
 * AP 写转发生端到端测试：双节点，非 owner 节点发布 → owner 生效，
 * 转发节点同步 apply（写后即读），乐观锁跨节点生效，owner 不可用时明确报错。
 */
public class ApForwardIntegrationTest {

    private static final String MEMBERS =
            "node-1=127.0.0.1:18848:19848,node-2=127.0.0.1:18849:19849";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldForwardWriteToOwnerAndApplyLocally() throws Exception {
        Path dataDir1 = folder.newFolder("fwd-node1").toPath();
        Path dataDir2 = folder.newFolder("fwd-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18848, 19848, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18849, 19849, dataDir2);
        try {
            DistroAssignments assignments = new DistroAssignments(Arrays.asList(
                    new ClusterMember("node-1", "127.0.0.1", 18848, 19848),
                    new ClusterMember("node-2", "127.0.0.1", 18849, 19849)));
            boolean ownerIsNode1 = "node-1"
                    .equals(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
            PolyConfigServer owner = ownerIsNode1 ? node1 : node2;
            PolyConfigServer other = ownerIsNode1 ? node2 : node1;
            int ownerHttp = owner.getHttpPort();
            int otherHttp = other.getHttpPort();

            // 经非 owner 发布：转发生效
            Response publish = post(otherHttp, "/v1/cs/configs",
                    publishBody("v1", null));
            assertEquals(200, publish.status);
            JSONObject published = JSON.parseObject(publish.body).getJSONObject("data");
            assertEquals("version 应由 owner 分配", 1L, published.getLongValue("version"));

            // owner 上读得到
            Response onOwner = get(ownerHttp,
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, onOwner.status);
            assertEquals("v1", JSON.parseObject(onOwner.body)
                    .getJSONObject("data").getString("content"));

            // 转发节点同步 apply：写后即读，本地立即可见且版本一致
            Response onOther = get(otherHttp,
                    "/v1/cs/configs?namespace=public&group=DEFAULT_GROUP&dataId=app.yml");
            assertEquals(200, onOther.status);
            JSONObject otherView = JSON.parseObject(onOther.body).getJSONObject("data");
            assertEquals("v1", otherView.getString("content"));
            assertEquals(1L, otherView.getLongValue("version"));

            // 乐观锁跨节点：基于 version=1 再发布成功；过期 expectedVersion 冲突
            Response updated = post(otherHttp, "/v1/cs/configs",
                    publishBody("v2", 1L));
            assertEquals(200, updated.status);
            Response stale = post(otherHttp, "/v1/cs/configs",
                    publishBody("v3", 1L));
            assertEquals(409, stale.status);
            assertEquals(40901, JSON.parseObject(stale.body).getIntValue("code"));

            // 经 owner 直接发布同样生效（省转发）
            Response direct = post(ownerHttp, "/v1/cs/configs",
                    publishBody("v3", 2L));
            assertEquals(200, direct.status);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void shouldRejectWriteWhenOwnerDown() throws Exception {
        Path dataDir1 = folder.newFolder("down-node1").toPath();
        Path dataDir2 = folder.newFolder("down-node2").toPath();
        PolyConfigServer node1 = startNode("node-1", 18848, 19848, dataDir1);
        PolyConfigServer node2 = startNode("node-2", 18849, 19849, dataDir2);
        DistroAssignments assignments = new DistroAssignments(Arrays.asList(
                new ClusterMember("node-1", "127.0.0.1", 18848, 19848),
                new ClusterMember("node-2", "127.0.0.1", 18849, 19849)));
        boolean ownerIsNode1 = "node-1"
                .equals(assignments.ownerOf("public", "DEFAULT_GROUP").getNodeId());
        try {
            // 停掉 owner：另一节点对该分区的写必须明确失败（owner 故障转移不在 v1）
            (ownerIsNode1 ? node1 : node2).stop();
            PolyConfigServer survivor = ownerIsNode1 ? node2 : node1;
            Response rejected = post(survivor.getHttpPort(), "/v1/cs/configs",
                    publishBody("v1", null));
            assertEquals(503, rejected.status);
        } finally {
            node1.stop();
            node2.stop();
        }
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
                .clusterId("ap-fwd-test")
                .mode("ap")
                .clusterMembers(MEMBERS)
                .build());
        server.start();
        return server;
    }

    private static String publishBody(String content, Long expectedVersion) {
        JSONObject request = new JSONObject();
        request.put("namespace", "public");
        request.put("group", "DEFAULT_GROUP");
        request.put("dataId", "app.yml");
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
