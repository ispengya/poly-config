package com.ispengya.polyconfig.storage.log;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link SegmentLog} 单元测试。
 */
public class SegmentLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldAppendReadAndScan() throws Exception {
        Path dir = folder.newFolder("basic").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);

        try (SegmentLog log = new SegmentLog(config)) {
            long first = log.append(bytes("alpha"));
            long second = log.append(bytes("beta"));

            assertEquals(first + SegmentLog.FRAME_HEADER_SIZE + 5, second);
            assertArrayEquals(bytes("alpha"), log.read(first));
            assertArrayEquals(bytes("beta"), log.read(second));

            final List<String> values = new ArrayList<>();
            log.scan((offset, payload) -> {
                values.add(new String(payload, StandardCharsets.UTF_8));
                return true;
            });
            assertEquals(Arrays.asList("alpha", "beta"), values);
            assertEquals(second + SegmentLog.FRAME_HEADER_SIZE + 4, log.getMaxOffset());
        }
    }

    @Test
    public void shouldRollSegmentsAndRecover() throws Exception {
        Path dir = folder.newFolder("roll").toPath();
        SegmentLogConfig config = config(dir, 64, 32);
        byte[] payload = new byte[20];
        long[] offsets = new long[5];

        try (SegmentLog log = new SegmentLog(config)) {
            for (int i = 0; i < offsets.length; i++) {
                payload[0] = (byte) i;
                offsets[i] = log.append(payload);
            }
            assertEquals(offsets[4] + SegmentLog.FRAME_HEADER_SIZE + payload.length,
                    log.getMaxOffset());
        }

        int segmentCount = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path ignored : stream) {
                segmentCount++;
            }
        }
        assertTrue("expected multiple segments", segmentCount > 1);

        try (SegmentLog log = new SegmentLog(config)) {
            assertEquals(offsets[4] + SegmentLog.FRAME_HEADER_SIZE + payload.length,
                    log.getMaxOffset());
            final AtomicInteger count = new AtomicInteger();
            log.scan((offset, bytes) -> {
                count.incrementAndGet();
                return true;
            });
            assertEquals(5, count.get());
            assertArrayEquals(payload, log.read(offsets[4]));
            assertEquals(-1L, log.getRecoveryTruncatedOffset());
        }
    }

    @Test
    public void shouldTruncateTornTailOnRecovery() throws Exception {
        Path dir = folder.newFolder("torn").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);
        long second;

        try (SegmentLog log = new SegmentLog(config)) {
            log.append(bytes("one"));
            second = log.append(bytes("two"));
            assertEquals(second + SegmentLog.FRAME_HEADER_SIZE + 3, log.getMaxOffset());
        }

        Path segment = onlySegment(dir);
        Files.write(segment, new byte[]{1, 2, 3, 4, 5}, StandardOpenOption.APPEND);

        try (SegmentLog log = new SegmentLog(config)) {
            long expectedEnd = second + SegmentLog.FRAME_HEADER_SIZE + 3;
            assertEquals(expectedEnd, log.getMaxOffset());
            assertEquals(expectedEnd, log.getRecoveryTruncatedOffset());

            final AtomicInteger count = new AtomicInteger();
            log.scan((offset, payload) -> {
                count.incrementAndGet();
                return true;
            });
            assertEquals(2, count.get());
        }
    }

    @Test
    public void shouldTruncateAtRecordBoundaryAndAppendAgain() throws Exception {
        Path dir = folder.newFolder("truncate").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);
        long first;
        long second;

        try (SegmentLog log = new SegmentLog(config)) {
            first = log.append(bytes("a"));
            second = log.append(bytes("b"));
            log.append(bytes("c"));

            log.truncateTo(second);
            assertEquals(second, log.getMaxOffset());
            assertArrayEquals(bytes("a"), log.read(first));

            try {
                log.read(second);
                fail("expected PolyConfigException");
            } catch (PolyConfigException e) {
                assertEquals(ErrorCode.STORAGE_ERROR, e.getErrorCode());
            }

            long appended = log.append(bytes("d"));
            assertEquals(second, appended);
            assertArrayEquals(bytes("d"), log.read(second));
        }

        try (SegmentLog log = new SegmentLog(config)) {
            final List<String> values = new ArrayList<>();
            log.scan((offset, payload) -> {
                values.add(new String(payload, StandardCharsets.UTF_8));
                return true;
            });
            assertEquals(Arrays.asList("a", "d"), values);
        }
    }

    @Test
    public void shouldRejectTruncateAtNonBoundary() throws Exception {
        Path dir = folder.newFolder("non-boundary").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);

        try (SegmentLog log = new SegmentLog(config)) {
            log.append(bytes("a"));
            log.append(bytes("b"));
            try {
                log.truncateTo(1L);
                fail("expected PolyConfigException");
            } catch (PolyConfigException e) {
                assertEquals(ErrorCode.INVALID_ARGUMENT, e.getErrorCode());
            }
        }
    }

    @Test
    public void shouldTruncateAtCorruptedRecordOnRecovery() throws Exception {
        Path dir = folder.newFolder("corrupt").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);
        long second;
        long third;

        try (SegmentLog log = new SegmentLog(config)) {
            log.append(bytes("aaa"));
            second = log.append(bytes("bbb"));
            third = log.append(bytes("ccc"));
            assertEquals(third + SegmentLog.FRAME_HEADER_SIZE + 3, log.getMaxOffset());
        }

        Path segment = onlySegment(dir);
        long payloadPosition = second + SegmentLog.FRAME_HEADER_SIZE + 1;
        try (FileChannel channel = FileChannel.open(segment,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            channel.read(buffer, payloadPosition);
            buffer.flip();
            byte current = buffer.get();
            buffer.clear();
            buffer.put((byte) (current ^ 0xFF));
            buffer.flip();
            channel.write(buffer, payloadPosition);
            channel.force(true);
        }

        try (SegmentLog log = new SegmentLog(config)) {
            assertEquals(second, log.getMaxOffset());
            assertEquals(second, log.getRecoveryTruncatedOffset());

            final AtomicInteger count = new AtomicInteger();
            log.scan((offset, payload) -> {
                count.incrementAndGet();
                return true;
            });
            assertEquals(1, count.get());
        }
    }

    @Test
    public void shouldRejectSecondOpenOnSameDir() throws Exception {
        Path dir = folder.newFolder("locked").toPath();
        SegmentLogConfig config = config(dir, 1024, 128);

        SegmentLog first = new SegmentLog(config);
        try {
            new SegmentLog(config);
            fail("expected PolyConfigException");
        } catch (PolyConfigException e) {
            assertEquals(ErrorCode.STORAGE_ERROR, e.getErrorCode());
        } finally {
            first.close();
        }

        try (SegmentLog reopened = new SegmentLog(config)) {
            reopened.append(bytes("after-reopen"));
            assertEquals(1, countRecords(reopened));
        }
    }

    @Test
    public void shouldQuarantineSubsequentSegmentsOnGap() throws Exception {
        Path dir = folder.newFolder("gap-quarantine").toPath();
        SegmentLogConfig config = config(dir, 64, 32);
        byte[] payload = new byte[20];

        List<String> before;
        try (SegmentLog log = new SegmentLog(config)) {
            for (int i = 0; i < 4; i++) {
                payload[0] = (byte) i;
                log.append(payload);
            }
            before = segmentNames(dir);
            assertTrue("expected multiple segments", before.size() >= 3);
        }

        // 删掉第二个段文件，制造"文件缺口"
        Files.delete(dir.resolve(before.get(1)));

        try (SegmentLog log = new SegmentLog(config)) {
            // 缺口在第二个段：被删除的段本身不计，其后的段全部隔离
            assertEquals(before.size() - 2, log.getQuarantinedFiles().size());
            assertEquals(before.get(2), log.getQuarantinedFiles().get(0));
            // 第一个段仍然可用，缺口之后的段被隔离而非静默删除
            assertEquals(SegmentLog.FRAME_HEADER_SIZE + 20, log.getMaxOffset());
            assertEquals(SegmentLog.FRAME_HEADER_SIZE + 20, log.getRecoveryTruncatedOffset());
        }

        // 被隔离的段文件在 quarantine 目录中保留（原名 + 时间戳后缀）
        Path quarantineDir = dir.resolve(".quarantined");
        assertTrue(Files.isDirectory(quarantineDir));
        List<String> quarantinedNames = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(quarantineDir)) {
            for (Path file : stream) {
                quarantinedNames.add(file.getFileName().toString());
            }
        }
        assertEquals(before.size() - 2, quarantinedNames.size());
        for (String quarantined : quarantinedNames) {
            boolean matched = false;
            for (String original : before) {
                if (quarantined.startsWith(original + ".")) {
                    matched = true;
                    break;
                }
            }
            assertTrue("quarantined file should derive from an original segment: " + quarantined,
                    matched);
        }
    }

    private static List<String> segmentNames(Path dir) throws IOException {
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path file : stream) {
                names.add(file.getFileName().toString());
            }
        }
        names.sort(Comparator.naturalOrder());
        return names;
    }

    private static int countRecords(SegmentLog log) {
        final AtomicInteger count = new AtomicInteger();
        log.scan((offset, payload) -> {
            count.incrementAndGet();
            return true;
        });
        return count.get();
    }

    private static SegmentLogConfig config(Path dir, int segmentSize, int maxRecordSize) {
        return SegmentLogConfig.builder(dir)
                .segmentSize(segmentSize)
                .maxRecordSize(maxRecordSize)
                .build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Path onlySegment(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path file : stream) {
                return file;
            }
        }
        fail("expected a segment file");
        return null;
    }
}
