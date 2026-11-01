package com.ispengya.polyconfig.storage.snapshot;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * 快照存储。
 *
 * <p>快照写临时文件后原子替换；保留最近 2 份——日志前缀删除之后，
 * 唯一快照损坏将不可恢复，必须留有回退。加载时按 revision 从大到小
 * 逐个尝试，解析或校验失败的跳过。</p>
 */
public final class SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    private static final String FILE_PREFIX = "snapshot-";

    private static final String FILE_SUFFIX = ".json";

    /**
     * 保留的快照份数。
     */
    private static final int RETENTION = 2;

    private final Path dir;

    /**
     * 创建快照存储。
     *
     * @param dir 快照目录
     */
    public SnapshotStore(Path dir) {
        if (dir == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "快照目录不能为空");
        }
        this.dir = dir;
    }

    /**
     * 原子写入快照并清理旧份。
     *
     * @param data 快照内容
     */
    public void write(SnapshotData data) {
        if (data == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "快照内容不能为空");
        }
        data.setFormatVersion(SnapshotData.FORMAT_VERSION);
        data.setSavedAt(System.currentTimeMillis());
        Path target = dir.resolve(FILE_PREFIX
                + String.format(Locale.ROOT, "%020d", data.getRevision()) + FILE_SUFFIX);
        Path temp = dir.resolve(target.getFileName() + ".tmp");
        try {
            Files.createDirectories(dir);
            byte[] json = JsonUtils.toPrettyJson(data).getBytes(StandardCharsets.UTF_8);
            Files.write(temp, json, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            pruneOld();
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "写入快照失败: " + target, e);
        }
    }

    /**
     * 加载最新可用快照。
     *
     * @return 最新快照；目录为空或全部不可用时返回 null
     */
    public SnapshotData loadLatest() {
        if (!Files.isDirectory(dir)) {
            return null;
        }
        for (Path file : listSnapshots()) {
            try {
                String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
                SnapshotData data = JsonUtils.parseObject(json, SnapshotData.class);
                if (data == null
                        || data.getFormatVersion() != SnapshotData.FORMAT_VERSION
                        || data.getRevision() <= 0
                        || data.getLogOffset() < 0
                        || data.getEntries() == null) {
                    log.warn("快照校验失败，跳过: {}", file.getFileName());
                    continue;
                }
                return data;
            } catch (RuntimeException | IOException e) {
                log.warn("快照解析失败，跳过: {}", file.getFileName(), e);
            }
        }
        return null;
    }

    private void pruneOld() {
        List<Path> files = listSnapshots();
        for (int i = RETENTION; i < files.size(); i++) {
            try {
                Files.deleteIfExists(files.get(i));
            } catch (IOException e) {
                log.warn("清理旧快照失败: {}", files.get(i).getFileName(), e);
            }
        }
    }

    /**
     * 按文件名中的 revision 从大到小列出快照文件。
     */
    private List<Path> listSnapshots() {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return files;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, FILE_PREFIX + "*" + FILE_SUFFIX)) {
            for (Path file : stream) {
                if (file.getFileName().toString().endsWith(FILE_SUFFIX)) {
                    files.add(file);
                }
            }
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "列出快照失败: " + dir, e);
        }
        files.sort(Comparator.comparing(Path::getFileName).reversed());
        return files;
    }
}
