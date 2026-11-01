package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * 启动配置默认值与参数解析测试（ISSUE-05）。
 */
public class ServerOptionsTest {

    @Test
    public void shouldDefaultHostToLocalhost() {
        ServerOptions options = ServerOptions.builder().build();
        assertEquals("127.0.0.1", options.getHost());
    }

    @Test
    public void shouldParseHostFromArgs() {
        ServerOptions options = ServerOptions.fromArgs(new String[]{"--host=0.0.0.0"});
        assertEquals("0.0.0.0", options.getHost());
    }

    @Test
    public void shouldRejectBlankHost() {
        try {
            ServerOptions.builder().host("  ").build();
            fail("空 host 应被拒绝");
        } catch (PolyConfigException expected) {
            // 预期
        }
    }

    @Test
    public void shouldDefaultSubscriptionLimitAndBodyBytes() {
        ServerOptions options = ServerOptions.builder().build();
        assertEquals(1024, options.getMaxSubscriptionsPerConnection());
        assertEquals(1024 * 1024L, options.getMaxBodyBytes());
    }

    @Test
    public void shouldParseSubscriptionLimitAndBodyBytesFromArgs() {
        ServerOptions options = ServerOptions.fromArgs(new String[]{
                "--max-subscriptions-per-connection=64",
                "--max-body-bytes=4096"});
        assertEquals(64, options.getMaxSubscriptionsPerConnection());
        assertEquals(4096L, options.getMaxBodyBytes());
    }

    @Test
    public void shouldRejectInvalidSubscriptionLimitAndBodyBytes() {
        try {
            ServerOptions.builder().maxSubscriptionsPerConnection(-1).build();
            fail("负数订阅上限应被拒绝");
        } catch (PolyConfigException expected) {
            // 预期
        }
        try {
            ServerOptions.builder().maxBodyBytes(0).build();
            fail("0 请求体上限应被拒绝");
        } catch (PolyConfigException expected) {
            // 预期
        }
    }
}
