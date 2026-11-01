package com.ispengya.polyconfig.client;

import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.util.Md5Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 客户端 revision 单调守卫与按 key 串行回调测试（ISSUE-02 / ISSUE-03）。
 */
public class PolyConfigClientTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PolyConfigClient client;

    @Before
    public void setUp() throws Exception {
        Path snapshotDir = folder.newFolder("snapshots").toPath();
        ClientOptions options = ClientOptions.builder()
                // 死端口：addListener 的初始订阅会失败，但被其内部捕获，不影响监听器注册
                .server("127.0.0.1:1")
                .snapshotDir(snapshotDir)
                .requestTimeoutMillis(100L)
                .resyncIntervalMillis(60000L)
                .build();
        client = new PolyConfigClient(options);
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void shouldDropStaleView() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.addListener("public", "DEFAULT_GROUP", "app.yml",
                (dataId, group, content) -> {
                    received.add(content);
                    latch.countDown();
                });

        assertTrue(client.applyView(view("v5", 5), false));
        // 乱序到达的旧视图必须被丢弃，不覆盖缓存、不触发回调
        assertFalse(client.applyView(view("v3", 3), false));
        assertTrue(client.applyView(view("v6", 6), true));

        assertTrue("回调超时", latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, received.size());
        assertEquals("v6", received.get(0));
    }

    @Test
    public void shouldNotifySameKeyInOrder() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);
        client.addListener("public", "DEFAULT_GROUP", "app.yml",
                (dataId, group, content) -> {
                    received.add(content);
                    latch.countDown();
                });

        assertTrue(client.applyView(view("a", 1), true));
        assertTrue(client.applyView(view("b", 2), true));
        assertTrue(client.applyView(view("c", 3), true));

        assertTrue("回调超时", latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, received.size());
        assertEquals("a", received.get(0));
        assertEquals("b", received.get(1));
        assertEquals("c", received.get(2));
    }

    @Test
    public void shouldDropStaleDeletion() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch deleted = new CountDownLatch(1);
        client.addListener("public", "DEFAULT_GROUP", "app.yml",
                (dataId, group, content) -> {
                    received.add(content);
                    if (content == null) {
                        deleted.countDown();
                    }
                });

        assertTrue(client.applyView(view("v5", 5), false));
        // 过期删除：不允许抹掉较新的内容，也不触发回调
        client.applyDeletion(new ConfigKey("public", "DEFAULT_GROUP", "app.yml"), 3);
        assertTrue(client.applyView(view("v6", 6), true));
        // 有效删除
        client.applyDeletion(new ConfigKey("public", "DEFAULT_GROUP", "app.yml"), 7);

        assertTrue("删除回调超时", deleted.await(5, TimeUnit.SECONDS));
        assertEquals(2, received.size());
        assertEquals("v6", received.get(0));
        assertNull(received.get(1));
    }

    @Test
    public void shouldRegisterListenerWithoutBlockingWhenServerUnavailable() throws Exception {
        ClientOptions options = ClientOptions.builder()
                .server("127.0.0.1:1")
                .snapshotDir(folder.newFolder("offline-snapshots").toPath())
                .requestTimeoutMillis(3000L)
                .resyncIntervalMillis(60000L)
                .build();
        PolyConfigClient offline = new PolyConfigClient(options);
        try {
            ConfigListener listener = (dataId, group, content) -> { };
            long start = System.currentTimeMillis();
            offline.addListener("public", "DEFAULT_GROUP", "offline.yml", listener);
            long elapsed = System.currentTimeMillis() - start;
            assertTrue("addListener 不应同步等待初始订阅, 耗时=" + elapsed + "ms",
                    elapsed < 1000);
            // 移除监听时的取消订阅同样是异步的，不抛异常
            offline.removeListener("public", "DEFAULT_GROUP", "offline.yml", listener);
        } finally {
            offline.close();
        }
    }

    @Test
    public void shouldApplyDeletionWithoutRevision() throws Exception {
        CountDownLatch deleted = new CountDownLatch(1);
        client.addListener("public", "DEFAULT_GROUP", "app.yml",
                (dataId, group, content) -> {
                    if (content == null) {
                        deleted.countDown();
                    }
                });

        assertTrue(client.applyView(view("v1", 1), false));
        // revision 未知时保持旧行为：直接删除
        client.applyDeletion(new ConfigKey("public", "DEFAULT_GROUP", "app.yml"), 0);

        assertTrue("删除回调超时", deleted.await(5, TimeUnit.SECONDS));
    }

    private static ConfigView view(String content, long version) {
        ConfigView view = new ConfigView();
        view.setNamespace("public");
        view.setGroup("DEFAULT_GROUP");
        view.setDataId("app.yml");
        view.setContent(content);
        view.setMd5(Md5Utils.md5Hex(content));
        // 单调守卫以跨节点权威的 version 为准（revision 是节点本地位点）
        view.setVersion(version);
        view.setRevision(version);
        return view;
    }
}
