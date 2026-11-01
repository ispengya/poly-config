package com.ispengya.polyconfig.server.push;

import com.ispengya.polyconfig.common.model.ConfigKey;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 订阅注册表测试：单键取消订阅与单连接订阅上限。
 */
public class SubscriptionRegistryTest {

    private static final ConfigKey KEY_A = new ConfigKey("public", "G", "a.yml");
    private static final ConfigKey KEY_B = new ConfigKey("public", "G", "b.yml");

    @Test
    public void shouldUnsubscribeSingleKey() {
        SubscriptionRegistry registry = new SubscriptionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

        assertTrue(registry.subscribe(channel, KEY_A));
        assertTrue(registry.subscribe(channel, KEY_B));
        assertEquals(2, registry.subscriptionCount());

        registry.unsubscribe(channel, KEY_A);
        assertTrue(registry.channels(KEY_A).isEmpty());
        assertEquals(1, registry.channels(KEY_B).size());
        assertEquals(1, registry.subscriptionCount());

        // 已取消的键再次取消不报错
        registry.unsubscribe(channel, KEY_A);
        assertEquals(1, registry.subscriptionCount());
    }

    @Test
    public void shouldClearAllOnRemove() {
        SubscriptionRegistry registry = new SubscriptionRegistry();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

        assertTrue(registry.subscribe(channel, KEY_A));
        assertTrue(registry.subscribe(channel, KEY_B));
        registry.remove(channel);

        assertTrue(registry.channels(KEY_A).isEmpty());
        assertTrue(registry.channels(KEY_B).isEmpty());
        assertEquals(0, registry.subscriptionCount());
    }

    @Test
    public void shouldRejectWhenSubscriptionLimitExceeded() {
        SubscriptionRegistry registry = new SubscriptionRegistry(2);
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

        assertTrue(registry.subscribe(channel, KEY_A));
        assertTrue(registry.subscribe(channel, KEY_B));
        // 超过上限
        assertFalse(registry.subscribe(channel, new ConfigKey("public", "G", "c.yml")));
        assertEquals(2, registry.subscriptionCount());
        // 重复订阅已有键不占新额度
        assertTrue(registry.subscribe(channel, KEY_A));
        assertEquals(2, registry.subscriptionCount());

        // 取消一个后可以再订阅新的
        registry.unsubscribe(channel, KEY_A);
        assertTrue(registry.subscribe(channel, new ConfigKey("public", "G", "c.yml")));
        assertEquals(2, registry.subscriptionCount());
    }

    @Test
    public void shouldTreatZeroLimitAsUnlimited() {
        SubscriptionRegistry registry = new SubscriptionRegistry(0);
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

        for (int i = 0; i < 100; i++) {
            assertTrue(registry.subscribe(channel, new ConfigKey("public", "G", "k" + i)));
        }
        assertEquals(100, registry.subscriptionCount());
    }

    @Test
    public void shouldSnapshotConnectionSubscriptions() {
        SubscriptionRegistry registry = new SubscriptionRegistry();
        EmbeddedChannel channelA = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel channelB = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

        assertTrue(registry.subscribe(channelA, KEY_B));
        assertTrue(registry.subscribe(channelA, KEY_A));
        assertTrue(registry.subscribe(channelB, KEY_A));

        SubscriptionSnapshot snapshot = registry.snapshot();
        assertEquals(3, snapshot.getTotal());
        assertEquals(2, snapshot.getConnections().size());

        SubscriptionSnapshot.Connection channelAView = null;
        SubscriptionSnapshot.Connection channelBView = null;
        for (SubscriptionSnapshot.Connection connection : snapshot.getConnections()) {
            if (connection.getCount() == 2) {
                channelAView = connection;
            } else {
                channelBView = connection;
            }
        }
        // 键按字典序排序：a.yml 在前
        assertEquals("public/G/a.yml", channelAView.getKeys().get(0));
        assertEquals("public/G/b.yml", channelAView.getKeys().get(1));
        assertEquals(1, channelBView.getCount());
        assertEquals("public/G/a.yml", channelBView.getKeys().get(0));
    }
}
