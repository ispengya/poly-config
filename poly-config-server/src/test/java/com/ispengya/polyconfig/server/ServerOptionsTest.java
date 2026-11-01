package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    @Test
    public void shouldReportParameterNameWhenIntegerParseFails() {
        try {
            ServerOptions.fromArgs(new String[]{"--http-port=abc"});
            fail("非法整数应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含参数名: " + expected.getMessage(),
                    expected.getMessage().contains("http-port"));
            assertTrue("错误信息应包含原始值: " + expected.getMessage(),
                    expected.getMessage().contains("abc"));
        }
    }

    @Test
    public void shouldReportParameterNameWhenLongParseFails() {
        try {
            ServerOptions.fromArgs(new String[]{"--max-body-bytes=1.5MB"});
            fail("非法长整数应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含参数名: " + expected.getMessage(),
                    expected.getMessage().contains("max-body-bytes"));
        }
    }

    @Test
    public void shouldReportParameterNameWhenPathIsInvalid() {
        try {
            ServerOptions.fromArgs(new String[]{"--data-dir=a\u0000b"});
            fail("非法路径应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含参数名: " + expected.getMessage(),
                    expected.getMessage().contains("data-dir"));
        }
    }

    @Test
    public void shouldDefaultToStandaloneWithoutMembers() {
        ServerOptions options = ServerOptions.builder().build();
        assertEquals("standalone", options.getMode());
        assertTrue(options.getClusterMembers().isEmpty());
    }

    @Test
    public void shouldParseClusterMembersFromArgs() {
        ServerOptions options = ServerOptions.fromArgs(new String[]{
                "--mode=ap",
                "--node-id=n1",
                "--cluster-members=n1=127.0.0.1:8848:9848,n2=127.0.0.1:8849:9849"});
        assertEquals("ap", options.getMode());
        assertEquals(2, options.getClusterMembers().size());
        assertEquals("n1", options.getClusterMembers().get(0).getNodeId());
        assertEquals("127.0.0.1", options.getClusterMembers().get(0).getHost());
        assertEquals(8848, options.getClusterMembers().get(0).getHttpPort());
        assertEquals(9848, options.getClusterMembers().get(0).getRemotingPort());
        assertEquals("n2", options.getClusterMembers().get(1).getNodeId());
        assertEquals(8849, options.getClusterMembers().get(1).getHttpPort());
        assertEquals(9849, options.getClusterMembers().get(1).getRemotingPort());
    }

    @Test
    public void shouldRejectMalformedClusterMember() {
        try {
            ServerOptions.fromArgs(new String[]{
                    "--cluster-members=n1=127.0.0.1:8848"});
            fail("缺少 remoting 端口的成员项应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含格式提示: " + expected.getMessage(),
                    expected.getMessage().contains("nodeId=host:httpPort:remotingPort"));
        }
        try {
            ServerOptions.fromArgs(new String[]{
                    "--cluster-members=n1=127.0.0.1:8848:99999"});
            fail("越界端口应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含端口范围: " + expected.getMessage(),
                    expected.getMessage().contains("1..65535"));
        }
    }

    @Test
    public void shouldRejectDuplicateMemberNodeIds() {
        try {
            ServerOptions.fromArgs(new String[]{
                    "--cluster-members=n1=127.0.0.1:8848:9848,n1=127.0.0.1:8849:9849"});
            fail("重复的节点 ID 应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含重复的节点 ID: " + expected.getMessage(),
                    expected.getMessage().contains("n1"));
        }
    }

    @Test
    public void shouldRequireClusterMembersInApMode() {
        try {
            ServerOptions.fromArgs(new String[]{"--mode=ap"});
            fail("AP 模式缺少成员列表应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含参数名: " + expected.getMessage(),
                    expected.getMessage().contains("cluster-members"));
        }
    }

    @Test
    public void shouldRequireSelfInClusterMembers() {
        try {
            ServerOptions.fromArgs(new String[]{
                    "--mode=ap",
                    "--cluster-members=n2=127.0.0.1:8849:9849"});
            fail("成员列表缺少本节点应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含本节点 ID: " + expected.getMessage(),
                    expected.getMessage().contains("node-1"));
        }
    }

    @Test
    public void shouldRejectUnknownMode() {
        try {
            ServerOptions.fromArgs(new String[]{"--mode=rpc"});
            fail("未知模式应被拒绝");
        } catch (PolyConfigException expected) {
            assertTrue("错误信息应包含原始值: " + expected.getMessage(),
                    expected.getMessage().contains("rpc"));
        }
    }
}
