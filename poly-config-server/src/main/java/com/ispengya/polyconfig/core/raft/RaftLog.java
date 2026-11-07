package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.util.Preconditions;
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Raft 日志：在 {@link SegmentLog} 底座上提供 index/term 一等语义。
 *
 * <p>记录格式为 16 字节前缀（index 8B + term 8B）+ 命令 payload；恢复扫描时
 * 校验 index 从首个记录起逐号连续，不连续视为损坏直接拒绝启动（SegmentLog
 * 只会保留连续有效前缀，出现不连续只能是逻辑错误或 CRC 无法识别的介质损坏）。</p>
 *
 * <p>内存维护 {@code offsets/terms} 两张平行表（位置 = index - firstIndex），
 * term 查询 O(1) 不落盘；payload 只在读取时按位点取。前缀压缩通过
 * {@link #compactBefore} 删除已覆盖的完整段并修剪内存表，压缩后首个段可能
 * 从非 0 物理位点开始（{@code allowNonZeroStart}）。</p>
 *
 * <p>设计上由 RaftNode 单线程信箱调用；方法上的 {@code synchronized} 是
 * 防御性兜底，不是并发设计。</p>
 *
 * <p>本类不持久化 commitIndex（见 cp-design.md D3）：恢复后由 RaftNode 依据
 * 快照位点与 leader 交互重新确定。</p>
 */
public final class RaftLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

    /**
     * 记录前缀长度：index(8) + term(8)。
     */
    static final int RECORD_HEADER_SIZE = 16;

    /**
     * 分段日志底座。
     */
    private final SegmentLog segmentLog;

    /**
     * 各记录的物理位点，位置 = index - firstIndex。
     */
    private final ArrayList<Long> offsets = new ArrayList<>();

    /**
     * 各 index 的 term，与 offsets 平行。
     */
    private final ArrayList<Long> terms = new ArrayList<>();

    /**
     * 可用日志的最小 index。空日志为 1；前缀压缩后为首个保留记录的 index。
     */
    private long firstIndex = 1;

    /**
     * 在指定目录打开 Raft 日志（底座段日志位于 {@code baseDir} 本身）。
     *
     * @param baseDir 日志目录
     */
    public RaftLog(Path baseDir) {
        this(SegmentLogConfig.builder(baseDir)
                .allowNonZeroStart(true)
                .maxRecordSize(SegmentLogConfig.DEFAULT_MAX_RECORD_SIZE + RECORD_HEADER_SIZE)
                .build());
    }

    /**
     * 以显式配置打开 Raft 日志。
     *
     * @param config 段日志配置
     */
    public RaftLog(SegmentLogConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.segmentLog = new SegmentLog(config);
        try {
            recover();
        } catch (RuntimeException e) {
            // 打开成功但恢复失败：释放文件锁，避免数据目录被无声占住
            segmentLog.close();
            throw e;
        }
    }

    /**
     * 追加一条日志（index 必然是 lastIndex + 1，由本方法保证）。
     *
     * @param term   产生该条目的 leader 任期
     * @param payload 命令编码字节
     * @return 分配的 index
     */
    public synchronized long append(long term, byte[] payload) {
        if (term < 1) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "term 必须 ≥ 1: " + term);
        }
        Preconditions.checkNotNull(payload, "payload");
        long index = lastIndex() + 1;
        byte[] record = encodeRecord(index, term, payload);
        long offset = segmentLog.append(record);
        offsets.add(offset);
        terms.add(term);
        return index;
    }

    /**
     * 按 index 读取日志条目。
     *
     * @param index 目标 index，必须在 [firstIndex, lastIndex] 内
     * @return 日志条目
     */
    public synchronized RaftLogEntry read(long index) {
        int position = positionOf(index);
        byte[] record = segmentLog.read(offsets.get(position));
        if (record.length < RECORD_HEADER_SIZE) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "Raft 日志记录过短: index=" + index);
        }
        long recordIndex = decodeIndex(record);
        if (recordIndex != index) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "Raft 日志位点 index 错位: 期望 " + index + "，实际 " + recordIndex);
        }
        byte[] payload = Arrays.copyOfRange(record, RECORD_HEADER_SIZE, record.length);
        return new RaftLogEntry(index, terms.get(position), payload);
    }

    /**
     * 查询指定 index 的 term。
     *
     * @param index 目标 index，必须在 [firstIndex, lastIndex] 内
     * @return term
     */
    public synchronized long termAt(long index) {
        return terms.get(positionOf(index));
    }

    /**
     * 截断 index 之后的全部条目（含 index 本身之后）。
     *
     * <p>冲突回退用：保留 [firstIndex, index]，物理与内存同步裁剪。
     * 调用方（RaftNode）保证 index ≥ firstIndex——闸2 的 prevLogIndex
     * 必须可读才可能走到截断，不会截掉首个可用条目。</p>
     *
     * @param index 保留的最后一个 index
     */
    public synchronized void truncateAfter(long index) {
        if (offsets.isEmpty()) {
            if (index < firstIndex - 1) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "截断位点低于日志起点: " + index + " < " + (firstIndex - 1));
            }
            return;
        }
        if (index < firstIndex) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "截断位点低于日志起点: " + index + " < " + firstIndex);
        }
        if (index >= lastIndex()) {
            return;
        }
        int keep = (int) (index - firstIndex) + 1;
        int dropped = offsets.size() - keep;
        long physical = offsets.get(keep);
        segmentLog.truncateTo(physical);
        offsets.subList(keep, offsets.size()).clear();
        terms.subList(keep, terms.size()).clear();
        log.warn("Raft 日志截断: 保留至 index={}，丢弃 {} 条", index, dropped);
    }

    /**
     * 压缩已覆盖的前缀：删除物理完整段并修剪内存表。
     *
     * <p>仅删除"段末尾 &lt;= coveredUpTo"的完整段（活跃段永不删），
     * 因此 coveredIndex 与最后一个保留记录之间可能残留若干物理记录——
     * 重启扫描后 firstIndex 可能比本方法返回时略小，属预期：
     * 调用方（RaftNode）以快照 lastIncludedIndex 为准忽略这些条目。</p>
     *
     * @param coveredIndex 快照已覆盖到的 index（含）
     * @return 从内存表移除的条目数（物理删除的段数见段日志日志）
     */
    public synchronized int compactBefore(long coveredIndex) {
        if (offsets.isEmpty() || coveredIndex < firstIndex) {
            return 0;
        }
        long capped = Math.min(coveredIndex, lastIndex());
        int position = (int) (capped - firstIndex);
        // 覆盖位点 = 被覆盖的最后一个记录的结束位点（下一条记录的起点，或日志末尾）
        long coveredUpTo = position + 1 < offsets.size()
                ? offsets.get(position + 1)
                : segmentLog.getMaxOffset();
        segmentLog.deleteSegmentsBefore(coveredUpTo);

        long minOffset = segmentLog.getMinOffset();
        int removed = 0;
        while (!offsets.isEmpty() && offsets.get(0) < minOffset) {
            offsets.remove(0);
            terms.remove(0);
            firstIndex++;
            removed++;
        }
        if (removed > 0) {
            log.info("Raft 日志前缀压缩: coveredIndex={}, 移除 {} 条, firstIndex={}",
                    coveredIndex, removed, firstIndex);
        }
        return removed;
    }

    /**
     * 强制把日志刷到磁盘（应答复制/投票前调用）。
     */
    public synchronized void flush() {
        segmentLog.flush();
    }

    /**
     * 返回可用日志的最小 index（空日志为 1）。
     */
    public synchronized long firstIndex() {
        return firstIndex;
    }

    /**
     * 返回最后一条日志的 index（空日志为 firstIndex - 1）。
     */
    public synchronized long lastIndex() {
        return firstIndex + offsets.size() - 1;
    }

    /**
     * 返回最后一条日志的 term（空日志为 0，对应 index 0 的哨兵约定）。
     */
    public synchronized long lastTerm() {
        return terms.isEmpty() ? 0L : terms.get(terms.size() - 1);
    }

    /**
     * 返回日志是否为空。
     */
    public synchronized boolean isEmpty() {
        return offsets.isEmpty();
    }

    /**
     * 返回当前可用条目数。
     */
    public synchronized int size() {
        return offsets.size();
    }

    @Override
    public synchronized void close() {
        segmentLog.close();
    }

    private int positionOf(long index) {
        if (offsets.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "日志为空，无法访问 index=" + index);
        }
        if (index < firstIndex || index > lastIndex()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "index 越界: " + index + "，可用范围 [" + firstIndex + ", "
                            + lastIndex() + "]（低于 firstIndex 的条目已被快照覆盖）");
        }
        return (int) (index - firstIndex);
    }

    private void recover() {
        long minOffset = segmentLog.getMinOffset();
        segmentLog.scanFrom(minOffset, (offset, payload) -> {
            if (payload.length < RECORD_HEADER_SIZE) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "Raft 日志记录过短: offset=" + offset);
            }
            long index = decodeIndex(payload);
            long term = decodeTerm(payload);
            if (offsets.isEmpty()) {
                if (index < 1) {
                    throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                            "Raft 日志起始 index 非法: " + index);
                }
                firstIndex = index;
            } else if (index != firstIndex + offsets.size()) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "Raft 日志 index 不连续: 期望 " + (firstIndex + offsets.size())
                                + "，实际 " + index + "（日志损坏，拒绝启动）");
            }
            if (term < 1) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "Raft 日志 term 非法: " + term + " @ index=" + index);
            }
            offsets.add(offset);
            terms.add(term);
            return true;
        });
        if (!offsets.isEmpty()) {
            log.info("Raft 日志恢复: firstIndex={}, lastIndex={}, lastTerm={}",
                    firstIndex, lastIndex(), lastTerm());
        }
    }

    private static byte[] encodeRecord(long index, long term, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE + payload.length);
        buffer.putLong(index);
        buffer.putLong(term);
        buffer.put(payload);
        return buffer.array();
    }

    private static long decodeIndex(byte[] record) {
        return ByteBuffer.wrap(record, 0, 8).getLong();
    }

    private static long decodeTerm(byte[] record) {
        return ByteBuffer.wrap(record, 8, 8).getLong();
    }

    /**
     * 仅供测试：返回全部可用条目（按 index 升序）。
     */
    synchronized List<RaftLogEntry> snapshotEntries() {
        List<RaftLogEntry> result = new ArrayList<>(offsets.size());
        for (long index = firstIndex; index <= lastIndex(); index++) {
            result.add(read(index));
        }
        return result;
    }
}
