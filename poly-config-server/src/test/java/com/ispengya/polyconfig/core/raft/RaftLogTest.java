package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link RaftLog} 单元测试（CP-1 验收：逐号恢复、截断持久化、前缀压缩、损坏拒启）。
 */
public class RaftLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldAppendReadAndRecoverOnReopen() throws Exception {
        Path dir = folder.newFolder("recover").toPath();
        RaftLog raftLog = new RaftLog(dir);
        assertEquals(1, raftLog.firstIndex());
        assertEquals(0, raftLog.lastIndex());
        assertEquals(0, raftLog.lastTerm());

        long i1 = raftLog.append(1, bytes("cmd-a"));
        long i2 = raftLog.append(1, bytes("cmd-b"));
        long i3 = raftLog.append(2, bytes("cmd-c"));
        assertEquals(1, i1);
        assertEquals(2, i2);
        assertEquals(3, i3);
        assertEquals(2, raftLog.lastTerm());
        raftLog.flush();
        raftLog.close();

        RaftLog reopened = new RaftLog(dir);
        assertEquals(1, reopened.firstIndex());
        assertEquals(3, reopened.lastIndex());
        assertEquals(2, reopened.lastTerm());
        assertEquals(1, reopened.termAt(1));
        assertEquals(1, reopened.termAt(2));
        assertEquals(2, reopened.termAt(3));
        assertArrayEquals(bytes("cmd-a"), reopened.read(1).getPayload());
        assertArrayEquals(bytes("cmd-b"), reopened.read(2).getPayload());
        assertArrayEquals(bytes("cmd-c"), reopened.read(3).getPayload());
        assertEquals(1, reopened.read(1).getIndex());
        assertEquals(2, reopened.read(3).getTerm());
        // 恢复后追加从 lastIndex+1 继续
        assertEquals(4, reopened.append(2, bytes("cmd-d")));
        reopened.close();
    }

    @Test
    public void shouldTruncateTailAndKeepItAfterReopen() throws Exception {
        Path dir = folder.newFolder("truncate").toPath();
        RaftLog raftLog = new RaftLog(dir);
        for (int i = 1; i <= 5; i++) {
            raftLog.append(1, bytes("cmd-" + i));
        }
        // 模拟冲突回退：丢弃 index 4、5
        raftLog.truncateAfter(3);
        assertEquals(3, raftLog.lastIndex());
        assertEquals(1, raftLog.lastTerm());
        raftLog.close();

        RaftLog reopened = new RaftLog(dir);
        assertEquals(3, reopened.lastIndex());
        assertArrayEquals(bytes("cmd-3"), reopened.read(3).getPayload());
        try {
            reopened.read(4);
            fail("index 4 应已被截断");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        // 截断后追加落在 4，且可携带新 term（换代覆盖）
        assertEquals(4, reopened.append(2, bytes("cmd-4-new")));
        assertEquals(2, reopened.termAt(4));
        reopened.close();

        RaftLog again = new RaftLog(dir);
        assertEquals(4, again.lastIndex());
        assertEquals(2, again.lastTerm());
        assertArrayEquals(bytes("cmd-4-new"), again.read(4).getPayload());
        again.close();
    }

    @Test
    public void shouldRejectTruncateBelowFirstIndex() throws Exception {
        Path dir = folder.newFolder("truncate-bounds").toPath();
        RaftLog raftLog = new RaftLog(dir);
        raftLog.append(1, bytes("cmd-a"));
        try {
            raftLog.truncateAfter(0);
            fail("截断到 firstIndex 之下应被拒绝（不支持整表清空，清空归快照加载）");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        assertEquals(1, raftLog.lastIndex());
        raftLog.close();

        // 空日志上截断到 firstIndex-1（0）是 no-op
        Path emptyDir = folder.newFolder("truncate-empty").toPath();
        RaftLog empty = new RaftLog(emptyDir);
        empty.truncateAfter(0);
        assertEquals(0, empty.lastIndex());
        try {
            empty.truncateAfter(-1);
            fail("负 index 应被拒绝");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        empty.close();
    }

    @Test
    public void shouldRejectReadOutsideAvailableRange() throws Exception {
        Path dir = folder.newFolder("bounds").toPath();
        RaftLog raftLog = new RaftLog(dir);
        try {
            raftLog.read(1);
            fail("空日志读取应失败");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        raftLog.append(1, bytes("cmd-a"));
        try {
            raftLog.termAt(2);
            fail("越界 termAt 应失败");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        raftLog.close();
    }

    @Test
    public void shouldCompactPrefixAndRecoverFromNonZeroStart() throws Exception {
        // 小段尺寸保证多段：每条记录 16+8 字节，段 128 字节 → 每段约 5 条
        Path dir = folder.newFolder("compact").toPath();
        SegmentLogConfig config = SegmentLogConfig.builder(dir)
                .segmentSize(128)
                .maxRecordSize(64)
                .allowNonZeroStart(true)
                .build();
        RaftLog raftLog = new RaftLog(config);
        for (int i = 1; i <= 30; i++) {
            raftLog.append(1, bytes("cmd-" + i));
        }
        assertEquals(30, raftLog.lastIndex());

        int removed = raftLog.compactBefore(20);
        assertTrue("应删除若干完整段，实际 removed=" + removed, removed > 0);
        long firstAfterCompact = raftLog.firstIndex();
        assertTrue("firstIndex 应前进，实际 " + firstAfterCompact, firstAfterCompact > 1);
        // 被压缩区间不可读，保留区间可读
        try {
            raftLog.read(firstAfterCompact - 1);
            fail("已覆盖条目不应可读");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        assertArrayEquals(bytes("cmd-30"), raftLog.read(30).getPayload());
        assertEquals(31, raftLog.append(1, bytes("cmd-31")));
        raftLog.close();

        // 重启：物理残留（未满段中的覆盖区尾部记录）可能让 firstIndex 略小于
        // 压缩时位点——覆盖位点之后的条目必须仍可读，更早的由快照位点裁决
        RaftLog reopened = new RaftLog(dir);
        assertTrue("重启后 firstIndex 不得越过覆盖位点: " + reopened.firstIndex(),
                reopened.firstIndex() <= firstAfterCompact);
        assertEquals(31, reopened.lastIndex());
        assertArrayEquals(bytes("cmd-" + firstAfterCompact),
                reopened.read(firstAfterCompact).getPayload());
        assertArrayEquals(bytes("cmd-31"), reopened.read(31).getPayload());
        reopened.close();
    }

    @Test
    public void shouldResetToSnapshotBoundaryAndContinueAppending() throws Exception {
        // CP-4：InstallSnapshot 后整表清空、起点对齐 lastIncludedIndex+1
        Path dir = folder.newFolder("reset-snapshot").toPath();
        RaftLog raftLog = new RaftLog(dir);
        for (int i = 1; i <= 6; i++) {
            raftLog.append(1, bytes("cmd-" + i));
        }
        raftLog.resetToSnapshot(4);
        assertEquals(5, raftLog.firstIndex());
        assertEquals(4, raftLog.lastIndex());
        assertTrue("重置后应为空日志", raftLog.isEmpty());
        try {
            raftLog.read(4);
            fail("覆盖位点不应可读");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, expected.getErrorCode());
        }
        // 下一条追加落在快照边界之后，逐号连续
        assertEquals(5, raftLog.append(2, bytes("cmd-5-new")));
        assertEquals(6, raftLog.append(2, bytes("cmd-6-new")));
        assertEquals(2, raftLog.termAt(5));
        raftLog.close();

        // 重置后的追加已持久化：重启按记录内嵌 index 重建出快照边界
        RaftLog reopened = new RaftLog(dir);
        assertEquals(5, reopened.firstIndex());
        assertEquals(6, reopened.lastIndex());
        assertArrayEquals(bytes("cmd-5-new"), reopened.read(5).getPayload());

        // 再次重置（如恢复期发现日志落后快照）：整表清空、边界重对齐
        reopened.resetToSnapshot(4);
        assertTrue(reopened.isEmpty());
        assertEquals(5, reopened.firstIndex());
        assertEquals(5, reopened.append(3, bytes("cmd-5-newer")));
        reopened.close();
    }

    @Test
    public void shouldDiscardAllKeepingFirstIndex() throws Exception {
        // CP-4：冲突点恰在保留起点时整表丢弃，firstIndex 坐标不变、追加对齐
        Path dir = folder.newFolder("discard-all").toPath();
        RaftLog raftLog = new RaftLog(dir);
        for (int i = 1; i <= 6; i++) {
            raftLog.append(1, bytes("cmd-" + i));
        }
        raftLog.resetToSnapshot(4);
        assertEquals(5, raftLog.append(2, bytes("old-5")));
        assertEquals(6, raftLog.append(2, bytes("old-6")));

        raftLog.discardAll();
        assertTrue(raftLog.isEmpty());
        assertEquals("firstIndex 坐标应保持", 5, raftLog.firstIndex());
        assertEquals("清空后追加应落在 firstIndex", 5, raftLog.append(3, bytes("new-5")));
        assertEquals(3, raftLog.termAt(5));
        raftLog.close();
    }

    @Test
    public void shouldRejectIndexGapOnRecovery() throws Exception {
        Path dir = folder.newFolder("gap").toPath();
        // 直接用段日志构造 index 不连续的记录（1 跳到 3）
        SegmentLog segmentLog = new SegmentLog(SegmentLogConfig.builder(dir).build());
        segmentLog.append(record(1, 1, bytes("cmd-a")));
        segmentLog.append(record(3, 1, bytes("cmd-c")));
        segmentLog.flush();
        segmentLog.close();

        try {
            new RaftLog(dir);
            fail("index 不连续应拒绝启动");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
            assertTrue(expected.getMessage().contains("不连续"));
        }
    }

    @Test
    public void shouldRejectTooShortRecordOnRecovery() throws Exception {
        Path dir = folder.newFolder("short").toPath();
        SegmentLog segmentLog = new SegmentLog(SegmentLogConfig.builder(dir).build());
        // 一条合法记录 + 一条短于 16 字节头的记录
        segmentLog.append(record(1, 1, bytes("cmd-a")));
        segmentLog.append(new byte[]{1, 2, 3});
        segmentLog.flush();
        segmentLog.close();

        try {
            new RaftLog(dir);
            fail("过短记录应拒绝启动");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
        }
    }

    @Test
    public void shouldShareDirectoryLockWithSegmentLog() throws Exception {
        Path dir = folder.newFolder("lock").toPath();
        RaftLog raftLog = new RaftLog(dir);
        try {
            new RaftLog(dir);
            fail("同一目录重复打开应被拒绝");
        } catch (PolyConfigException expected) {
            assertEquals(ErrorCode.STORAGE_ERROR, expected.getErrorCode());
        }
        raftLog.close();
        // 关闭后可再次打开
        RaftLog reopened = new RaftLog(dir);
        reopened.close();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 构造 RaftLog 记录字节（index 8B + term 8B + payload）。
     */
    private static byte[] record(long index, long term, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(RaftLog.RECORD_HEADER_SIZE + payload.length);
        buffer.putLong(index);
        buffer.putLong(term);
        buffer.put(payload);
        return buffer.array();
    }
}
