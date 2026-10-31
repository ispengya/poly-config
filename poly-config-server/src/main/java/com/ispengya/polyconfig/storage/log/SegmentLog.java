package com.ispengya.polyconfig.storage.log;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * 基于分段文件顺序追加的日志。
 *
 * <p>帧格式为固定 16 字节头 + 可变长度 payload；头部包含魔数、版本、payload
 * 长度和 CRC32。CRC 覆盖头部前 12 字节与 payload，用于识别撕裂写入和内容损坏。</p>
 *
 * <p>日志以物理 offset 寻址，段文件名为 20 位补零的起始 offset。启动时按
 * "从 0 开始、连续前缀"的规则恢复：遇到损坏或缺口时截断到最后一个合法帧，
 * 后续段文件移入 {@code .quarantined} 目录隔离保留，不会静默删除。
 * 尾部撕裂写入会被直接截断，不会静默保留。</p>
 *
 * <p>打开日志时会在数据目录抢 {@code .store.lock} 文件锁；同一数据目录
 * 已被其他实例（含同进程重复打开）持有时直接拒绝启动，避免双开写坏日志。</p>
 */
public final class SegmentLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SegmentLog.class);

    /**
     * 数据目录文件锁名。
     */
    private static final String LOCK_FILE_NAME = ".store.lock";

    /**
     * 恢复截断时隔离段文件的子目录名。
     */
    private static final String QUARANTINE_DIR_NAME = ".quarantined";

    /**
     * 帧头长度。
     */
    static final int FRAME_HEADER_SIZE = 16;

    /**
     * 帧魔数 "PCLG"。
     */
    private static final int MAGIC = 0x50434C47;

    /**
     * 帧格式版本。
     */
    private static final byte VERSION = 1;

    /**
     * CRC 字段在帧头中的偏移。
     */
    private static final int CRC_FIELD_OFFSET = 12;

    /**
     * 日志段文件后缀。
     */
    private static final String SEGMENT_SUFFIX = ".log";

    /**
     * 日志配置。
     */
    private final SegmentLogConfig config;

    /**
     * 已打开的日志段，按起始 offset 排序。
     */
    private final TreeMap<Long, Segment> segments = new TreeMap<>();

    /**
     * 当前可写的日志段。
     */
    private Segment current;

    /**
     * 读写锁：追加/刷盘/截断持写锁；扫描只在捕获段清单时短暂持读锁，
     * 实际读取与 visitor 回调在锁外执行，长扫描不再阻塞写入（ISSUE-17）。
     */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * 日志合法末尾的物理 offset。
     */
    private volatile long maxOffset;

    /**
     * 本次启动发生恢复截断的物理 offset；-1 表示未发生截断。
     */
    private long recoveryTruncatedOffset = -1L;

    /**
     * 本次启动被隔离到 quarantine 目录的日志段文件名。
     */
    private final List<String> quarantinedFiles = new ArrayList<>();

    /**
     * 数据目录锁文件通道。
     */
    private FileChannel lockChannel;

    /**
     * 数据目录文件锁。
     */
    private FileLock lock;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * 打开或创建分段日志。
     *
     * @param config 日志配置
     */
    public SegmentLog(SegmentLogConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        if (config.getMaxRecordSize() + FRAME_HEADER_SIZE > config.getSegmentSize()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "maxRecordSize 过大，单帧无法放入日志段");
        }
        createBaseDir();
        acquireLock();
        try {
            recover();
        } catch (RuntimeException e) {
            releaseLockQuietly();
            throw e;
        }
    }

    /**
     * 追加一条记录。
     *
     * @param payload 记录内容，null 视为空记录
     * @return 记录起始物理 offset
     */
    public long append(byte[] payload) {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            byte[] value = payload == null ? new byte[0] : payload;
            if (value.length > config.getMaxRecordSize()) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "单条记录长度超过上限: " + value.length);
            }
            byte[] frame = encodeFrame(value);
            if (current == null || current.writePosition + frame.length > config.getSegmentSize()) {
                current = createSegment(maxOffset);
                segments.put(current.startOffset, current);
            }
            long offset = maxOffset;
            try {
                current.append(frame);
            } catch (IOException e) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "追加日志失败", e);
            }
            maxOffset += frame.length;
            if (config.isSyncOnAppend()) {
                flush();
            }
            return offset;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 按物理 offset 精确读取一条记录。
     *
     * @param physicalOffset 记录起始物理 offset
     * @return 记录内容
     */
    public byte[] read(long physicalOffset) {
        rwLock.readLock().lock();
        try {
            ensureOpen();
            Segment segment = findSegment(physicalOffset);
            if (segment == null) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "日志位点不存在: " + physicalOffset);
            }
            int localPosition = (int) (physicalOffset - segment.startOffset);
            if (localPosition < 0 || localPosition >= segment.writePosition) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "日志位点越界: " + physicalOffset);
            }
            DecodedFrame frame = readFrame(segment, localPosition, segment.writePosition, true);
            return frame.payload;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 从头扫描全部记录。
     *
     * @param visitor 记录访问器
     */
    public void scan(LogVisitor visitor) {
        scanFrom(0L, visitor);
    }

    /**
     * 从指定物理 offset 扫描记录。
     *
     * @param startOffset 起始物理 offset，必须是记录边界
     * @param visitor 记录访问器
     */
    public void scanFrom(long startOffset, LogVisitor visitor) {
        Preconditions.checkNotNull(visitor, "visitor");
        List<ScanTarget> targets;
        long limit;
        rwLock.readLock().lock();
        try {
            ensureOpen();
            if (startOffset < 0 || startOffset > maxOffset) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "扫描位点越界: " + startOffset);
            }
            // 锁内只捕获段清单与各自的有效长度，实际读取与 visitor 在锁外进行
            targets = new ArrayList<>(segments.size());
            for (Segment segment : segments.values()) {
                targets.add(new ScanTarget(segment, segment.writePosition));
            }
            limit = maxOffset;
        } finally {
            rwLock.readLock().unlock();
        }
        long cursor = startOffset;
        while (cursor < limit) {
            ScanTarget target = findTarget(targets, cursor);
            if (target == null) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "扫描时日志位点不存在: " + cursor);
            }
            int localPosition = (int) (cursor - target.segment.startOffset);
            DecodedFrame frame = readFrame(target.segment, localPosition, target.limit, true);
            if (!visitor.onRecord(cursor, frame.payload)) {
                return;
            }
            cursor += frame.totalLength;
        }
    }

    private static ScanTarget findTarget(List<ScanTarget> targets, long physicalOffset) {
        for (ScanTarget target : targets) {
            if (physicalOffset >= target.segment.startOffset
                    && physicalOffset < target.segment.startOffset + target.limit) {
                return target;
            }
        }
        return null;
    }

    /**
     * 截断到指定物理 offset。
     *
     * <p>调用方必须保证 offset 是记录边界；本方法会校验，不满足时抛异常。</p>
     *
     * @param physicalOffset 截断后的日志末尾
     */
    public void truncateTo(long physicalOffset) {
        rwLock.writeLock().lock();
        try {
            doTruncateTo(physicalOffset);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void doTruncateTo(long physicalOffset) {
        ensureOpen();
        if (physicalOffset < 0 || physicalOffset > maxOffset) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "截断位点越界: " + physicalOffset);
        }
        if (physicalOffset == maxOffset) {
            return;
        }
        if (physicalOffset == 0L) {
            closeAndDeleteAll();
            maxOffset = 0L;
            current = null;
            return;
        }

        Segment segment = findSegment(physicalOffset);
        if (segment == null) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "截断位点不存在: " + physicalOffset);
        }
        int localPosition = (int) (physicalOffset - segment.startOffset);
        validateBoundary(segment, localPosition);

        List<Long> removeStarts = new ArrayList<>(segments.tailMap(physicalOffset, true).keySet());
        for (Long start : removeStarts) {
            Segment removed = segments.remove(start);
            if (removed != null) {
                closeQuietly(removed);
                deleteQuietly(removed.path);
            }
        }

        Segment containing = segments.get(segment.startOffset);
        if (containing == null) {
            current = segments.isEmpty() ? null : segments.lastEntry().getValue();
        } else {
            try {
                containing.truncateTo(localPosition);
            } catch (IOException e) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "截断日志段失败", e);
            }
            current = containing;
        }
        maxOffset = physicalOffset;
    }

    /**
     * 强制把所有日志段刷到磁盘。
     */
    public void flush() {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            for (Segment segment : segments.values()) {
                try {
                    segment.flush();
                } catch (IOException e) {
                    throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                            "刷盘失败: " + segment.path, e);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 删除完全被快照覆盖的非活跃日志段。
     *
     * <p>只删除"段末尾 &lt;= coveredUpTo"的完整覆盖段，活跃段永不删除；
     * 调用方必须保证 coveredUpTo 之前的日志内容已通过快照持久化。</p>
     *
     * @param coveredUpTo 快照已覆盖到的物理 offset
     * @return 删除的段数
     */
    public int deleteSegmentsBefore(long coveredUpTo) {
        rwLock.writeLock().lock();
        try {
            ensureOpen();
            if (coveredUpTo < 0) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "覆盖位点非法: " + coveredUpTo);
            }
            List<Long> candidates = new ArrayList<>();
            for (Map.Entry<Long, Segment> entry : segments.entrySet()) {
                Segment segment = entry.getValue();
                if (segment == current) {
                    break;
                }
                if (segment.startOffset + segment.writePosition <= coveredUpTo) {
                    candidates.add(entry.getKey());
                } else {
                    break;
                }
            }
            for (Long start : candidates) {
                Segment removed = segments.remove(start);
                if (removed != null) {
                    closeQuietly(removed);
                    deleteQuietly(removed.path);
                }
            }
            return candidates.size();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 返回日志合法末尾的物理 offset。
     *
     * @return 末尾物理 offset
     */
    public long getMaxOffset() {
        return maxOffset;
    }

    /**
     * 返回本次启动发生恢复截断的物理 offset。
     *
     * @return 截断终点；-1 表示未发生截断
     */
    public long getRecoveryTruncatedOffset() {
        return recoveryTruncatedOffset;
    }

    /**
     * 返回本次启动被隔离到 quarantine 目录的日志段文件名（不含路径）。
     *
     * @return 隔离文件名列表，未发生隔离时为空列表
     */
    public List<String> getQuarantinedFiles() {
        return Collections.unmodifiableList(quarantinedFiles);
    }

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            PolyConfigException failure = null;
            for (Segment segment : segments.values()) {
                try {
                    segment.flush();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = new PolyConfigException(ErrorCode.STORAGE_ERROR,
                                "关闭前刷盘失败: " + segment.path, e);
                    }
                }
                try {
                    segment.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = new PolyConfigException(ErrorCode.STORAGE_ERROR,
                                "关闭日志段失败: " + segment.path, e);
                    }
                }
            }
            segments.clear();
            current = null;
            releaseLockQuietly();
            if (failure != null) {
                throw failure;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void createBaseDir() {
        try {
            Files.createDirectories(config.getBaseDir());
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "创建日志目录失败: " + config.getBaseDir(), e);
        }
    }

    private void acquireLock() {
        Path lockFile = config.getBaseDir().resolve(LOCK_FILE_NAME);
        try {
            lockChannel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                lock = lockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                lock = null;
            }
        } catch (IOException e) {
            releaseLockQuietly();
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "打开数据目录锁文件失败: " + lockFile, e);
        }
        if (lock == null) {
            releaseLockQuietly();
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "数据目录已被其他实例占用，拒绝启动: " + config.getBaseDir());
        }
    }

    private void releaseLockQuietly() {
        if (lock != null) {
            try {
                lock.release();
            } catch (IOException ignored) {
                // 锁释放失败不影响主流程，进程退出后 OS 会回收
            }
            lock = null;
        }
        if (lockChannel != null) {
            try {
                lockChannel.close();
            } catch (IOException ignored) {
                // 同上
            }
            lockChannel = null;
        }
    }

    private void recover() {
        List<Path> files = listSegmentFiles();
        if (files.isEmpty()) {
            return;
        }

        long firstStart = parseStart(files.get(0));
        if (firstStart != 0L && !config.isAllowNonZeroStart()) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "首个日志段必须从 0 开始: " + files.get(0));
        }
        long expectedStart = firstStart;
        for (int i = 0; i < files.size(); i++) {
            Path file = files.get(i);
            long start = parseStart(file);
            if (start != expectedStart) {
                markRecoveryTruncated(expectedStart);
                quarantineFrom(files, i,
                        "段起始 offset 不连续，期望 " + expectedStart + "，实际 " + start);
                break;
            }

            Segment segment = openSegment(file, start);
            long end = scanValidPrefix(segment);
            int validLength = (int) (end - start);
            long fileSize;
            try {
                fileSize = segment.size();
            } catch (IOException e) {
                closeQuietly(segment);
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "读取日志段大小失败: " + file, e);
            }
            if (validLength < fileSize) {
                try {
                    segment.truncateTo(validLength);
                } catch (IOException e) {
                    closeQuietly(segment);
                    throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                            "截断损坏日志段失败: " + file, e);
                }
                markRecoveryTruncated(end);
            }
            segment.writePosition = validLength;
            segment.flushedPosition = validLength;

            if (validLength == 0 && i > 0) {
                closeQuietly(segment);
                quarantine(file, "空日志段");
                markRecoveryTruncated(start);
                quarantineFrom(files, i + 1, "空日志段之后的日志段");
                break;
            }

            segments.put(start, segment);

            if (i + 1 < files.size()) {
                long nextStart = parseStart(files.get(i + 1));
                if (nextStart != end) {
                    markRecoveryTruncated(end);
                    quarantineFrom(files, i + 1,
                            "段起始 offset 不连续，期望 " + end + "，实际 " + nextStart);
                    break;
                }
            }
            expectedStart = end;
        }

        if (!segments.isEmpty()) {
            current = segments.lastEntry().getValue();
            maxOffset = current.startOffset + current.writePosition;
        } else {
            maxOffset = expectedStart;
        }
    }

    private long scanValidPrefix(Segment segment) {
        long fileSize;
        try {
            fileSize = segment.size();
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "读取日志段大小失败: " + segment.path, e);
        }
        int position = 0;
        long global = segment.startOffset;
        while (position + FRAME_HEADER_SIZE <= fileSize) {
            DecodedFrame frame = readFrame(segment, position, fileSize, false);
            if (frame == null) {
                break;
            }
            position += frame.totalLength;
            global += frame.totalLength;
        }
        return global;
    }

    private DecodedFrame readFrame(Segment segment, int localPosition, long limit, boolean strict) {
        if (localPosition < 0 || localPosition + FRAME_HEADER_SIZE > limit) {
            if (strict) {
                throw corrupted(segment, localPosition, "帧头不完整");
            }
            return null;
        }

        ByteBuffer header = ByteBuffer.allocate(FRAME_HEADER_SIZE);
        try {
            readFully(segment.channel, header, localPosition);
        } catch (IOException e) {
            if (strict) {
                throw corrupted(segment, localPosition, "读取帧头失败");
            }
            return null;
        }
        header.flip();
        int magic = header.getInt();
        byte version = header.get();
        header.get();
        header.getShort();
        int payloadLength = header.getInt();
        int expectedCrc = header.getInt();

        if (magic != MAGIC || version != VERSION
                || payloadLength < 0 || payloadLength > config.getMaxRecordSize()) {
            if (strict) {
                throw corrupted(segment, localPosition, "帧头非法");
            }
            return null;
        }

        int frameLength = FRAME_HEADER_SIZE + payloadLength;
        if (localPosition + frameLength > limit) {
            if (strict) {
                throw corrupted(segment, localPosition, "帧不完整");
            }
            return null;
        }

        byte[] payload = new byte[payloadLength];
        if (payloadLength > 0) {
            try {
                readFully(segment.channel, ByteBuffer.wrap(payload),
                        localPosition + FRAME_HEADER_SIZE);
            } catch (IOException e) {
                if (strict) {
                    throw corrupted(segment, localPosition, "读取 payload 失败");
                }
                return null;
            }
        }

        CRC32 crc = new CRC32();
        crc.update(header.array(), 0, CRC_FIELD_OFFSET);
        crc.update(payload, 0, payload.length);
        if ((int) crc.getValue() != expectedCrc) {
            if (strict) {
                throw corrupted(segment, localPosition, "CRC 校验失败");
            }
            return null;
        }
        return new DecodedFrame(payload, frameLength);
    }

    private void validateBoundary(Segment segment, int localPosition) {
        int position = 0;
        while (position < localPosition) {
            DecodedFrame frame = readFrame(segment, position, segment.writePosition, true);
            position += frame.totalLength;
        }
        if (position != localPosition) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "截断位点不是记录边界: " + (segment.startOffset + localPosition));
        }
    }

    private Segment findSegment(long physicalOffset) {
        Map.Entry<Long, Segment> entry = segments.floorEntry(physicalOffset);
        if (entry == null) {
            return null;
        }
        Segment segment = entry.getValue();
        if (physicalOffset >= segment.startOffset
                && physicalOffset < segment.startOffset + segment.writePosition) {
            return segment;
        }
        return null;
    }

    private Segment createSegment(long startOffset) {
        Path file = config.getBaseDir().resolve(segmentFileName(startOffset));
        try {
            FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            return new Segment(file, startOffset, channel);
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "创建日志段失败: " + file, e);
        }
    }

    private Segment openSegment(Path file, long startOffset) {
        try {
            FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            return new Segment(file, startOffset, channel);
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "打开日志段失败: " + file, e);
        }
    }

    private List<Path> listSegmentFiles() {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(config.getBaseDir())) {
            return files;
        }
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(config.getBaseDir(), "*" + SEGMENT_SUFFIX)) {
            for (Path file : stream) {
                String name = file.getFileName().toString();
                if (name.length() == 24 && isDigits(name.substring(0, 20))) {
                    files.add(file);
                }
            }
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "列出日志段失败: " + config.getBaseDir(), e);
        }
        files.sort(Comparator.comparingLong(SegmentLog::parseStart));
        return files;
    }

    private static long parseStart(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(0, 20));
    }

    private static boolean isDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current < '0' || current > '9') {
                return false;
            }
        }
        return true;
    }

    private static String segmentFileName(long startOffset) {
        return String.format(Locale.ROOT, "%020d%s", startOffset, SEGMENT_SUFFIX);
    }

    private void quarantineFrom(List<Path> files, int fromIndex, String reason) {
        for (int i = fromIndex; i < files.size(); i++) {
            quarantine(files.get(i), reason);
        }
    }

    private void quarantine(Path file, String reason) {
        Path quarantineDir = config.getBaseDir().resolve(QUARANTINE_DIR_NAME);
        Path target = quarantineDir.resolve(
                file.getFileName() + "." + System.currentTimeMillis());
        try {
            Files.createDirectories(quarantineDir);
            Files.move(file, target);
            quarantinedFiles.add(file.getFileName().toString());
            log.warn("恢复截断隔离日志段: {}，原因: {}，已移至: {}",
                    file.getFileName(), reason, target);
        } catch (IOException e) {
            // 移动失败时保留原文件，避免数据丢失；下次启动重试隔离
            log.error("隔离日志段失败，文件保留原位: {}，原因: {}", file, reason, e);
        }
    }

    private void closeAndDeleteAll() {
        for (Segment segment : segments.values()) {
            closeQuietly(segment);
            deleteQuietly(segment.path);
        }
        segments.clear();
    }

    private void markRecoveryTruncated(long offset) {
        if (recoveryTruncatedOffset < 0) {
            recoveryTruncatedOffset = offset;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志已关闭");
        }
    }

    private PolyConfigException corrupted(Segment segment, int localPosition, String reason) {
        return new PolyConfigException(ErrorCode.STORAGE_ERROR,
                "日志帧损坏: " + segment.path + " @" + (segment.startOffset + localPosition)
                        + " (" + reason + ")");
    }

    private static void closeQuietly(Segment segment) {
        try {
            segment.close();
        } catch (IOException ignored) {
            // 关闭失败不覆盖主流程异常
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // 清理失败不影响已确定的有效前缀
        }
    }

    private static byte[] encodeFrame(byte[] payload) {
        int frameLength = FRAME_HEADER_SIZE + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(frameLength);
        buffer.putInt(MAGIC);
        buffer.put(VERSION);
        buffer.put((byte) 0);
        buffer.putShort((short) 0);
        buffer.putInt(payload.length);

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, CRC_FIELD_OFFSET);
        crc.update(payload, 0, payload.length);

        buffer.putInt((int) crc.getValue());
        buffer.put(payload);
        return buffer.array();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) {
                throw new IOException("write returned " + written);
            }
            position += written;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new EOFException("unexpected end of file");
            }
            if (read == 0) {
                continue;
            }
            position += read;
        }
    }

    /**
     * 扫描时捕获的段目标：段引用 + 捕获时刻的有效长度。
     */
    private static final class ScanTarget {

        private final Segment segment;
        private final int limit;

        private ScanTarget(Segment segment, int limit) {
            this.segment = segment;
            this.limit = limit;
        }
    }

    /**
     * 已解码的日志帧。
     */
    private static final class DecodedFrame {

        /**
         * 帧内容。
         */
        private final byte[] payload;

        /**
         * 帧总长度。
         */
        private final int totalLength;

        private DecodedFrame(byte[] payload, int totalLength) {
            this.payload = payload;
            this.totalLength = totalLength;
        }
    }

    /**
     * 单个日志段。
     */
    private static final class Segment implements Closeable {

        /**
         * 段文件路径。
         */
        private final Path path;

        /**
         * 段起始物理 offset。
         */
        private final long startOffset;

        /**
         * 段文件通道。
         */
        private final FileChannel channel;

        /**
         * 当前写入位置。
         */
        private int writePosition;

        /**
         * 已刷盘位置。
         */
        private int flushedPosition;

        private Segment(Path path, long startOffset, FileChannel channel) {
            this.path = path;
            this.startOffset = startOffset;
            this.channel = channel;
        }

        private long size() throws IOException {
            return channel.size();
        }

        private void append(byte[] frame) throws IOException {
            writeFully(channel, ByteBuffer.wrap(frame), writePosition);
            writePosition += frame.length;
        }

        private void truncateTo(int length) throws IOException {
            if (channel.size() > length) {
                channel.truncate(length);
            }
            channel.force(true);
            writePosition = length;
            flushedPosition = Math.min(flushedPosition, length);
        }

        private void flush() throws IOException {
            if (writePosition > flushedPosition) {
                channel.force(false);
                flushedPosition = writePosition;
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
