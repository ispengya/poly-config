package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link RaftMetaStore} 单元测试（CP-1 验收：持久化、重载、term 不回退、损坏拒启）。
 */
public class RaftMetaStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldStartFreshWhenFileMissing() throws Exception {
        Path file = folder.newFolder("fresh").toPath().resolve("meta.json");
        RaftMetaStore store = new RaftMetaStore(file);
        assertEquals(0, store.currentTerm());
        assertNull(store.votedFor());
    }

    @Test
    public void shouldPersistAndReload() throws Exception {
        Path file = folder.newFolder("persist").toPath().resolve("meta.json");
        RaftMetaStore store = new RaftMetaStore(file);
        store.persist(5, "node-2");
        assertEquals(5, store.currentTerm());
        assertEquals("node-2", store.votedFor());

        // 重开（模拟崩溃重启）：值必须完整回来
        RaftMetaStore reopened = new RaftMetaStore(file);
        assertEquals(5, reopened.currentTerm());
        assertEquals("node-2", reopened.votedFor());

        // term 递增、投票清空（新一届尚未投票）
        reopened.persist(6, null);
        RaftMetaStore third = new RaftMetaStore(file);
        assertEquals(6, third.currentTerm());
        assertNull(third.votedFor());
        // 不残留临时文件
        assertTrue(Files.exists(file));
        try (java.util.stream.Stream<Path> files = Files.list(file.getParent())) {
            assertEquals(0, files.filter(p -> p.getFileName().toString().endsWith(".tmp")).count());
        }
    }

    @Test
    public void shouldRejectTermRegression() throws Exception {
        Path file = folder.newFolder("regress").toPath().resolve("meta.json");
        RaftMetaStore store = new RaftMetaStore(file);
        store.persist(5, "node-1");
        try {
            store.persist(4, "node-2");
            fail("term 回退应被拒绝");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        // 拒绝后内存值不变
        assertEquals(5, store.currentTerm());
        assertEquals("node-1", store.votedFor());
    }

    @Test
    public void shouldAllowSameTermRevoteToSameNode() throws Exception {
        Path file = folder.newFolder("revote").toPath().resolve("meta.json");
        RaftMetaStore store = new RaftMetaStore(file);
        store.persist(3, "node-1");
        // 幂等重复落盘
        store.persist(3, "node-1");
        assertEquals(3, store.currentTerm());
        // 同 term 换投票对象：存储层不拦（"一 term 一票"由 RaftNode 决策层保证，
        // 存储只负责把决策如实落盘）——文档化这一分层
        store.persist(3, "node-2");
        assertEquals("node-2", store.votedFor());
    }

    @Test
    public void shouldRejectCorruptFileOnStartup() throws Exception {
        Path dir = folder.newFolder("corrupt").toPath();
        Path file = dir.resolve("meta.json");
        Files.write(file, "{ not json".getBytes(StandardCharsets.UTF_8));
        try {
            new RaftMetaStore(file);
            fail("损坏的元数据应拒绝启动");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
        }

        // 格式版本不符同样拒绝
        Files.write(file, ("{\"formatVersion\":99,\"currentTerm\":1,\"votedFor\":null}")
                .getBytes(StandardCharsets.UTF_8));
        try {
            new RaftMetaStore(file);
            fail("未知格式版本应拒绝启动");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
        }

        // term 为负拒绝
        Files.write(file, ("{\"formatVersion\":1,\"currentTerm\":-1,\"votedFor\":null}")
                .getBytes(StandardCharsets.UTF_8));
        try {
            new RaftMetaStore(file);
            fail("负 term 应拒绝启动");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
        }
    }
}
