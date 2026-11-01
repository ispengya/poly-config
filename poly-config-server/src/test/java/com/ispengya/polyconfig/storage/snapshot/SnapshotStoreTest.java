package com.ispengya.polyconfig.storage.snapshot;

import com.ispengya.polyconfig.common.util.JsonUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link SnapshotStore} 落盘格式与兼容性测试。
 */
public class SnapshotStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldWritePrettyPrintedSnapshotAndReload() throws Exception {
        Path dir = folder.newFolder("snap").toPath();
        SnapshotStore store = new SnapshotStore(dir);

        SnapshotData data = new SnapshotData();
        data.setRevision(7);
        data.setLogOffset(128);
        data.setEntries(new ArrayList<>());
        store.write(data);

        Path file = dir.resolve("snapshot-00000000000000000007.json");
        assertTrue(Files.isRegularFile(file));
        String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        assertTrue("快照应为格式化 JSON（含换行）", json.contains("\n"));
        assertTrue("快照应包含缩进", json.matches("(?s).*\\n\\s+\"revision\".*"));

        SnapshotData loaded = store.loadLatest();
        assertNotNull(loaded);
        assertEquals(7L, loaded.getRevision());
        assertEquals(128L, loaded.getLogOffset());
        assertEquals(SnapshotData.FORMAT_VERSION, loaded.getFormatVersion());
    }

    @Test
    public void shouldLoadLegacyCompactSnapshot() throws Exception {
        Path dir = folder.newFolder("legacy-snap").toPath();
        SnapshotData data = new SnapshotData();
        data.setFormatVersion(SnapshotData.FORMAT_VERSION);
        data.setRevision(3);
        data.setLogOffset(64);
        data.setSavedAt(System.currentTimeMillis());
        data.setEntries(new ArrayList<>());
        // 旧版紧凑单行格式仍可加载
        String compact = JsonUtils.toJson(data);
        assertTrue("旧格式应为单行紧凑 JSON", !compact.contains("\n"));
        Files.write(dir.resolve("snapshot-00000000000000000003.json"),
                compact.getBytes(StandardCharsets.UTF_8));

        SnapshotData loaded = new SnapshotStore(dir).loadLatest();
        assertNotNull(loaded);
        assertEquals(3L, loaded.getRevision());
        assertEquals(64L, loaded.getLogOffset());
    }
}
