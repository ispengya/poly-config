package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Raft 持久化元数据：currentTerm 与 votedFor。
 *
 * <p>对应 Raft 恢复三件套中的两件（第三件是日志本身，见 {@link RaftLog}）。
 * {@link #persist} 采用"写临时文件 → fsync → 原子替换"，<b>返回即已落盘</b>——
 * 投票与拉票消息必须在本方法返回后才能发出（cp-design.md D3：先落盘再投票）。</p>
 *
 * <p>文件损坏时拒绝启动（fail-stop）：带着丢失的 votedFor 启动可能在同一
 * term 重复投票，破坏"一 term 一票"——宁可起不来，不能投错票。</p>
 */
public final class RaftMetaStore {

    private static final Logger log = LoggerFactory.getLogger(RaftMetaStore.class);

    /**
     * 元数据文件格式版本。
     */
    public static final int FORMAT_VERSION = 1;

    private final Path file;

    private long currentTerm;

    private String votedFor;

    /**
     * 打开元数据存储；文件不存在时初始化为 (0, null)。
     *
     * @param file 元数据文件路径（建议 <data-dir>/raft/meta.json）
     */
    public RaftMetaStore(Path file) {
        if (file == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "元数据文件路径不能为空");
        }
        this.file = file;
        load();
    }

    /**
     * 返回当前任期。
     */
    public synchronized long currentTerm() {
        return currentTerm;
    }

    /**
     * 返回本 term 投票对象（null 表示尚未投票）。
     */
    public synchronized String votedFor() {
        return votedFor;
    }

    /**
     * 持久化 term/vote；返回前保证写入已 fsync 到磁盘。
     *
     * @param newTerm   新任期，必须 ≥ 当前任期（term 永不回退）
     * @param newVotedFor 本 term 投票对象，null 表示清空（仅随 term 递增发生）
     */
    public synchronized void persist(long newTerm, String newVotedFor) {
        if (newTerm < currentTerm) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "term 不得回退: " + newTerm + " < " + currentTerm);
        }
        if (newTerm == currentTerm && Objects.equals(newVotedFor, votedFor)) {
            return;
        }
        if (newVotedFor != null && newVotedFor.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "votedFor 不能为空串");
        }

        MetaFile meta = new MetaFile();
        meta.setFormatVersion(FORMAT_VERSION);
        meta.setCurrentTerm(newTerm);
        meta.setVotedFor(newVotedFor);
        byte[] json = JsonUtils.toPrettyJson(meta).getBytes(StandardCharsets.UTF_8);

        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.createDirectories(file.getParent());
            try (FileChannel channel = FileChannel.open(temp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(json);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                // fsync 后才允许原子替换——替换成功即代表持久化完成
                channel.force(true);
            }
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "写入 Raft 元数据失败: " + file, e);
        }

        this.currentTerm = newTerm;
        this.votedFor = newVotedFor;
        log.info("Raft 元数据落盘: term={}, votedFor={}", newTerm, newVotedFor);
    }

    private void load() {
        if (!Files.exists(file)) {
            currentTerm = 0L;
            votedFor = null;
            return;
        }
        MetaFile meta;
        try {
            String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            meta = JsonUtils.parseObject(json, MetaFile.class);
        } catch (IOException | RuntimeException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "Raft 元数据损坏，拒绝启动: " + file, e);
        }
        if (meta == null || meta.getFormatVersion() != FORMAT_VERSION
                || meta.getCurrentTerm() < 0
                || (meta.getVotedFor() != null && meta.getVotedFor().isEmpty())) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "Raft 元数据校验失败，拒绝启动: " + file);
        }
        this.currentTerm = meta.getCurrentTerm();
        this.votedFor = meta.getVotedFor();
        log.info("Raft 元数据加载: term={}, votedFor={}", currentTerm, votedFor);
    }

    /**
     * 元数据文件 DTO（fastjson2）。
     */
    public static final class MetaFile {

        private int formatVersion;

        private long currentTerm;

        private String votedFor;

        public int getFormatVersion() {
            return formatVersion;
        }

        public void setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
        }

        public long getCurrentTerm() {
            return currentTerm;
        }

        public void setCurrentTerm(long currentTerm) {
            this.currentTerm = currentTerm;
        }

        public String getVotedFor() {
            return votedFor;
        }

        public void setVotedFor(String votedFor) {
            this.votedFor = votedFor;
        }
    }
}
