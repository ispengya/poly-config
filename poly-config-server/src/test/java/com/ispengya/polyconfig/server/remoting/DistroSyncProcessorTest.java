package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * 副本侧复制批次幂等应用测试：比本地新才应用，重复/乱序/缺失删除无害。
 */
public class DistroSyncProcessorTest {

    private static final ConfigKey KEY = new ConfigKey("public", "DEFAULT_GROUP", "app.yml");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LocalEngine engine;
    private DistroSyncProcessor processor;

    @Before
    public void setUp() throws Exception {
        Path dataDir = folder.newFolder("sync-engine").toPath();
        engine = new LocalEngine(dataDir, "node-2", "sync-test", 100, 0);
        processor = new DistroSyncProcessor(engine);
    }

    @After
    public void tearDown() {
        engine.close();
    }

    @Test
    public void shouldApplyNewerAndSkipStaleOrDuplicate() {
        RemotingCommand response = sync(
                record(ConfigOperation.PUBLISH, "public", "DEFAULT_GROUP", "app.yml", "v2", 2L),
                // 同批重复与过期版本都必须丢弃
                record(ConfigOperation.PUBLISH, "public", "DEFAULT_GROUP", "app.yml", "v2-dup", 2L),
                record(ConfigOperation.PUBLISH, "public", "DEFAULT_GROUP", "app.yml", "v1-stale", 1L));
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        assertEquals("applied=1, skipped=2", new String(response.getBody(), StandardCharsets.UTF_8));

        ConfigEntry entry = engine.read(KEY).get();
        assertEquals("v2", entry.getContent());
        assertEquals(2L, entry.getVersion());
    }

    @Test
    public void shouldApplyDeleteAndSkipRepeatedOrAbsent() {
        engine.proposeLocal(new ConfigCommand(ConfigOperation.PUBLISH, KEY, "v1",
                ConfigType.YAML, "tester", System.currentTimeMillis()));
        long version = engine.read(KEY).get().getVersion();

        // 新版本删除 → 应用；重复删除 → 丢弃；本地不存在的删除 → 丢弃
        RemotingCommand response = sync(
                record(ConfigOperation.DELETE, "public", "DEFAULT_GROUP", "app.yml", "", version + 1),
                record(ConfigOperation.DELETE, "public", "DEFAULT_GROUP", "app.yml", "", version + 1),
                record(ConfigOperation.DELETE, "public", "OTHER_GROUP", "ghost.yml", "", 9L));
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        assertEquals("applied=1, skipped=2", new String(response.getBody(), StandardCharsets.UTF_8));
        assertFalse("删除后不应有条目", engine.read(KEY).isPresent());
    }

    @Test
    public void shouldApplyDeleteOverLocalWithoutEntryCheck() {
        // 本地没有条目但带更高版本的发布必须应用（副本缺失前序同步的场景）
        RemotingCommand response = sync(
                record(ConfigOperation.PUBLISH, "public", "DEFAULT_GROUP", "late.yml", "v5", 5L));
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        assertEquals("applied=1, skipped=0", new String(response.getBody(), StandardCharsets.UTF_8));
        assertEquals(5L, engine.read(new ConfigKey("public", "DEFAULT_GROUP", "late.yml"))
                .get().getVersion());
    }

    @Test
    public void shouldRejectRecordWithoutAssignedVersion() {
        CommandRecord record = record(ConfigOperation.PUBLISH, "public", "DEFAULT_GROUP",
                "app.yml", "v1", null);
        RemotingCommand response = sync(record);
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST, response.getCode());
    }

    @Test
    public void shouldRejectEmptyBody() {
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_SYNC, null);
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST,
                processor.processRequest(null, request).getCode());
    }

    private static CommandRecord record(ConfigOperation operation, String namespace,
                                        String group, String dataId, String content,
                                        Long version) {
        CommandRecord record = new CommandRecord();
        record.setOperation(operation.name());
        record.setNamespace(namespace);
        record.setGroup(group);
        record.setDataId(dataId);
        record.setContent(content);
        record.setType("YAML");
        record.setOperator("owner");
        record.setTimestamp(System.currentTimeMillis());
        record.setVersion(version);
        return record;
    }

    private RemotingCommand sync(CommandRecord... records) {
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_SYNC, null);
        request.setBody(JsonUtils.toJson(DistroSyncRequest.from(
                java.util.Arrays.asList(records))).getBytes(StandardCharsets.UTF_8));
        return processor.processRequest(null, request);
    }
}
