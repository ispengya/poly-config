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
}
