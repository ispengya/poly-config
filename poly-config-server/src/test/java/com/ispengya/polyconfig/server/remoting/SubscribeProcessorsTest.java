package com.ispengya.polyconfig.server.remoting;

import com.alibaba.fastjson2.JSON;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeRequest;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeResponse;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SubscribeItem;
import com.ispengya.polyconfig.common.protocol.SubscribeRequest;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.polyconfig.core.standalone.LocalProtocol;
import com.ispengya.polyconfig.server.push.SubscriptionRegistry;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 订阅处理器测试：单键订阅的未变更短路与删除识别、批量对账的差异选择、订阅上限。
 */
public class SubscribeProcessorsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LocalProtocol protocol;
    private ConfigService configService;
    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;

    @Before
    public void setUp() throws Exception {
        Path dataDir = folder.newFolder("data").toPath();
        protocol = new LocalProtocol(dataDir, "node-1", "test-cluster", 100, 0);
        configService = new ConfigService(protocol);
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ctx = channel.pipeline().firstContext();
    }

    @After
    public void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        if (protocol != null) {
            protocol.close();
        }
    }

    @Test
    public void shouldRejectSubscribeOverConnectionLimit() {
        SubscriptionRegistry registry = new SubscriptionRegistry(1);
        SubscribeProcessor processor = new SubscribeProcessor(configService, registry);

        RemotingCommand first = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", "", 0L)));
        assertEquals(ProtocolCodes.RESPONSE_OK, first.getCode());

        RemotingCommand second = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "b.yml", "", 0L)));
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST, second.getCode());
        assertTrue(second.getRemark().contains("上限"));

        // 重复订阅已有键不占新额度
        RemotingCommand again = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", "", 0L)));
        assertEquals(ProtocolCodes.RESPONSE_OK, again.getCode());
    }

    @Test
    public void shouldSkipBodyWhenRevisionOrMd5Unchanged() {
        configService.publish("public", "G", "a.yml", "v1", "YAML", "tester");
        ConfigEntry entry = configService.get("public", "G", "a.yml").get();
        SubscribeProcessor processor = new SubscribeProcessor(
                configService, new SubscriptionRegistry());

        // revision 一致：空 body
        RemotingCommand byRevision = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", "", entry.getRevision())));
        assertEquals(ProtocolCodes.RESPONSE_OK, byRevision.getCode());
        assertEquals(null, byRevision.getBody());

        // md5 一致：空 body
        RemotingCommand byMd5 = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", entry.getMd5(), 0L)));
        assertEquals(ProtocolCodes.RESPONSE_OK, byMd5.getCode());
        assertEquals(null, byMd5.getBody());

        // 版本落后：返回完整视图
        RemotingCommand stale = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", "", Math.max(0L, entry.getRevision() - 1))));
        assertEquals(ProtocolCodes.RESPONSE_OK, stale.getCode());
        assertNotNull(stale.getBody());
        ConfigView view = JSON.parseObject(new String(stale.getBody(),
                StandardCharsets.UTF_8), ConfigView.class);
        assertEquals("v1", view.getContent());
    }

    @Test
    public void shouldSignalNotFoundWhenClientHoldsDeletedKey() {
        SubscribeProcessor processor = new SubscribeProcessor(
                configService, new SubscriptionRegistry());

        // 首次订阅且服务端无此键：成功、空 body（订阅尚不存在的配置是合法用法）
        RemotingCommand fresh = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "ghost.yml", "", 0L)));
        assertEquals(ProtocolCodes.RESPONSE_OK, fresh.getCode());

        // 客户端声明持有 revision 但服务端已无此键：视为已删除
        RemotingCommand deleted = processor.processRequest(ctx,
                request(ProtocolCodes.SUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "ghost.yml", "", 5L)));
        assertEquals(ProtocolCodes.RESPONSE_NOT_FOUND, deleted.getCode());
    }

    @Test
    public void shouldReturnOnlyDiffInBatch() {
        configService.publish("public", "G", "a.yml", "v1", "YAML", "tester");
        configService.publish("public", "G", "b.yml", "v1", "YAML", "tester");
        ConfigEntry a = configService.get("public", "G", "a.yml").get();
        ConfigEntry b = configService.get("public", "G", "b.yml").get();
        BatchSubscribeProcessor processor = new BatchSubscribeProcessor(configService);

        List<SubscribeItem> targets = new ArrayList<>();
        // 版本落后 → changed
        targets.add(SubscribeItem.of("public", "G", "a.yml", "", 0L));
        // revision 一致 → 跳过
        targets.add(SubscribeItem.of("public", "G", "b.yml", "", b.getRevision()));
        // 声明持有但服务端已无 → deleted
        targets.add(SubscribeItem.of("public", "G", "ghost.yml", "", 5L));
        // 首次订阅且服务端无此键 → 既不 changed 也不 deleted
        targets.add(SubscribeItem.of("public", "G", "fresh.yml", "", 0L));

        RemotingCommand response = processor.processRequest(ctx,
                request(ProtocolCodes.BATCH_SUBSCRIBE, BatchSubscribeRequest.of(targets)));
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        BatchSubscribeResponse diff = JSON.parseObject(new String(response.getBody(),
                StandardCharsets.UTF_8), BatchSubscribeResponse.class);
        assertEquals(1, diff.getChanged().size());
        assertEquals("a.yml", diff.getChanged().get(0).getDataId());
        assertEquals("v1", diff.getChanged().get(0).getContent());
        assertEquals(1, diff.getDeleted().size());
        assertEquals("ghost.yml", diff.getDeleted().get(0).getDataId());
    }

    @Test
    public void shouldRejectEmptyAndOversizedBatch() {
        BatchSubscribeProcessor processor = new BatchSubscribeProcessor(configService);

        RemotingCommand empty = processor.processRequest(ctx,
                request(ProtocolCodes.BATCH_SUBSCRIBE,
                        BatchSubscribeRequest.of(new ArrayList<>())));
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST, empty.getCode());

        List<SubscribeItem> oversized = new ArrayList<>();
        for (int i = 0; i <= BatchSubscribeProcessor.MAX_TARGETS_PER_BATCH; i++) {
            oversized.add(SubscribeItem.of("public", "G", "k" + i + ".yml", "", 0L));
        }
        RemotingCommand tooMany = processor.processRequest(ctx,
                request(ProtocolCodes.BATCH_SUBSCRIBE, BatchSubscribeRequest.of(oversized)));
        assertEquals(ProtocolCodes.RESPONSE_BAD_REQUEST, tooMany.getCode());
        assertTrue(tooMany.getRemark().contains("超限"));
    }

    @Test
    public void shouldUnsubscribeViaProcessor() {
        SubscriptionRegistry registry = new SubscriptionRegistry();
        SubscribeProcessor subscribe = new SubscribeProcessor(configService, registry);
        UnsubscribeProcessor unsubscribe = new UnsubscribeProcessor(registry);
        ConfigKey key = new ConfigKey("public", "G", "a.yml");

        subscribe.processRequest(ctx, request(ProtocolCodes.SUBSCRIBE,
                SubscribeRequest.of("public", "G", "a.yml", "", 0L)));
        assertEquals(1, registry.channels(key).size());

        RemotingCommand response = unsubscribe.processRequest(ctx,
                request(ProtocolCodes.UNSUBSCRIBE, SubscribeRequest.of(
                        "public", "G", "a.yml", "", 0L)));
        assertEquals(ProtocolCodes.RESPONSE_OK, response.getCode());
        assertTrue(registry.channels(key).isEmpty());
    }

    private static RemotingCommand request(int code, Object body) {
        RemotingCommand request = RemotingCommand.createRequestCommand(code, null);
        request.setBody(JsonUtils.toJson(body).getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
