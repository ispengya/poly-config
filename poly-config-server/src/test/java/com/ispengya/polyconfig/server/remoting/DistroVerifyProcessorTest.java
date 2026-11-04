package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.DistroVerifyRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.VerifyEntry;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.core.distro.DistroAssignments;
import com.ispengya.polyconfig.core.distro.VerifyAnomalyStats;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 周期校验比对逻辑测试（docs/ap-design.md §6）：owner 是真相源，
 * 缺失/落后补增量，墓碑识别删除，副本多余键记异常，未拥有分区忽略。
 */
public class DistroVerifyProcessorTest {

    private static final List<ClusterMember> MEMBERS = Arrays.asList(
            new ClusterMember("node-1", "127.0.0.1", 18848, 19848),
            new ClusterMember("node-2", "127.0.0.1", 18849, 19849),
            new ClusterMember("node-3", "127.0.0.1", 18850, 19850));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LocalEngine engine;
    private DistroVerifyProcessor processor;
    private DistroAssignments assignments;
    private VerifyAnomalyStats anomalyStats;

    @Before
    public void setUp() throws Exception {
        Path dataDir = folder.newFolder("verify-engine").toPath();
        engine = new LocalEngine(dataDir, "node-1", "verify-test", 100, 0);
        assignments = new DistroAssignments(MEMBERS);
        anomalyStats = new VerifyAnomalyStats();
        processor = new DistroVerifyProcessor(engine, assignments, "node-1", anomalyStats);
    }

    @After
    public void tearDown() {
        engine.close();
    }

    @Test
    public void shouldReturnEmptyDeltaWhenReplicaUpToDate() {
        String group = ownedGroup("node-1");
        engine.proposeLocal(publish(group, "app.yml", "v1"));
        assertEquals(1L, engine.read(key(group, "app.yml")).get().getVersion());

        List<CommandRecord> delta = deltaOf(verify(
                entry(group, "app.yml", 1L, false)));
        assertTrue("版本一致时应为空增量", delta.isEmpty());
    }

    @Test
    public void shouldReturnDeltaWhenReplicaMissingOrStale() {
        String group = ownedGroup("node-1");
        engine.proposeLocal(publish(group, "app.yml", "v1"));
        engine.proposeLocal(publish(group, "app.yml", "v2"));
        engine.proposeLocal(publish(group, "other.yml", "fresh"));

        // app.yml 副本停在 v1（落后）；other.yml 副本完全没有（缺失）
        List<CommandRecord> delta = deltaOf(verify(
                entry(group, "app.yml", 1L, false)));
        assertEquals(2, delta.size());

        CommandRecord stale = find(delta, "app.yml");
        assertEquals(ConfigOperation.PUBLISH.name(), stale.getOperation());
        assertEquals("v2", stale.getContent());
        assertEquals(2L, stale.getVersion().longValue());

        CommandRecord missing = find(delta, "other.yml");
        assertEquals(ConfigOperation.PUBLISH.name(), missing.getOperation());
        assertEquals("fresh", missing.getContent());
    }

    @Test
    public void shouldReturnTombstoneWhenReplicaStillHasKey() {
        String group = ownedGroup("node-1");
        engine.proposeLocal(publish(group, "doomed.yml", "v1"));
        engine.proposeLocal(new ConfigCommand(ConfigOperation.DELETE,
                key(group, "doomed.yml"), "", ConfigType.TEXT, "tester",
                System.currentTimeMillis()));
        // 删除后 owner 版本 = 2（本地分配路径 version = revision）

        List<CommandRecord> delta = deltaOf(verify(
                entry(group, "doomed.yml", 1L, false)));
        assertEquals(1, delta.size());
        CommandRecord tombstone = delta.get(0);
        assertEquals(ConfigOperation.DELETE.name(), tombstone.getOperation());
        assertEquals(2L, tombstone.getVersion().longValue());
    }

    @Test
    public void shouldReturnNothingWhenBothDeletedOrReplicaAbsent() {
        String group = ownedGroup("node-1");
        engine.proposeLocal(publish(group, "doomed.yml", "v1"));
        engine.proposeLocal(new ConfigCommand(ConfigOperation.DELETE,
                key(group, "doomed.yml"), "", ConfigType.TEXT, "tester",
                System.currentTimeMillis()));

        // 双方均已删除：墓碑版本不比较，无须下发
        assertTrue(deltaOf(verify(
                entry(group, "doomed.yml", 99L, true))).isEmpty());
        // 副本压根没有该键（已删或从未存在）：同样无须下发
        assertTrue(deltaOf(verify()).isEmpty());
    }

    @Test
    public void shouldSkipAnomalyWhenReplicaHasKeyOwnerNeverHad() {
        String group = ownedGroup("node-1");
        engine.proposeLocal(publish(group, "app.yml", "v1"));

        // 副本报上已同步的 app.yml + owner 从未有过的 ghost 键：
        // ghost 记异常、不下发（WARN 日志），应答仍 OK 且无增量
        List<CommandRecord> delta = deltaOf(verify(
                entry(group, "app.yml", 1L, false),
                entry(group, "ghost.yml", 7L, false)));
        assertTrue("owner 没有的键不应下发任何数据", delta.isEmpty());
        assertEquals("异常应计入统计供状态接口展示", 1, anomalyStats.getCount());
        assertTrue("异常详情应包含键路径",
                anomalyStats.getLastDetail().contains("ghost.yml"));
    }

    @Test
    public void shouldIgnoreEntriesForPartitionsNotOwned() {
        String foreignGroup = ownedGroup("node-2");
        // 该分区归 node-2，本处理器（node-1）应整组忽略
        List<CommandRecord> delta = deltaOf(verify(
                entry(foreignGroup, "foreign.yml", 5L, false)));
        assertTrue(delta.isEmpty());
    }

    @Test
    public void shouldRejectEmptyOrIllegalBody() {
        RemotingCommand empty = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_VERIFY, null);
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST,
                processor.processRequest(null, empty).getCode());

        RemotingCommand illegal = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_VERIFY, null);
        illegal.setBody("not-json".getBytes(StandardCharsets.UTF_8));
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST,
                processor.processRequest(null, illegal).getCode());
    }

    private static VerifyEntry entry(String group, String dataId, long version,
                                     boolean deleted) {
        return new VerifyEntry("public", group, dataId, version, deleted);
    }

    private static ConfigKey key(String group, String dataId) {
        return new ConfigKey("public", group, dataId);
    }

    private static ConfigCommand publish(String group, String dataId, String content) {
        return new ConfigCommand(ConfigOperation.PUBLISH, key(group, dataId), content,
                ConfigType.YAML, "tester", System.currentTimeMillis());
    }

    private RemotingCommand verify(VerifyEntry... entries) {
        DistroVerifyRequest request = DistroVerifyRequest.of("node-2",
                Arrays.asList(entries));
        RemotingCommand command = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_VERIFY, null);
        command.setBody(JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8));
        return processor.processRequest(null, command);
    }

    private static List<CommandRecord> deltaOf(RemotingCommand response) {
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        DistroSyncRequest sync = JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8),
                DistroSyncRequest.class);
        assertNotNull("应答应携带增量体", sync);
        return sync.getCommands() == null
                ? Collections.<CommandRecord>emptyList() : sync.getCommands();
    }

    private static CommandRecord find(List<CommandRecord> records, String dataId) {
        for (CommandRecord record : records) {
            if (dataId.equals(record.getDataId())) {
                return record;
            }
        }
        throw new AssertionError("增量中缺少 dataId=" + dataId);
    }

    /**
     * 找一个归属指定节点的分组（分区归属按 hash 分布，需要探测）。
     */
    private String ownedGroup(String nodeId) {
        for (int i = 0; i < 200; i++) {
            String group = "G" + i;
            if (nodeId.equals(assignments.ownerOf("public", group).getNodeId())) {
                return group;
            }
        }
        throw new IllegalStateException("未找到归属 " + nodeId + " 的分组");
    }
}
