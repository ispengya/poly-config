package com.ispengya.polyconfig.core.standalone;

import com.ispengya.polyconfig.common.codec.CommandCodec;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.NodeRole;
import com.ispengya.polyconfig.consistency.NodeStatus;
import com.ispengya.polyconfig.consistency.ProtocolMode;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.state.ConfigStateMachine;
import com.ispengya.polyconfig.core.workspace.WorkspaceRegistry;
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import com.ispengya.polyconfig.storage.snapshot.SnapshotData;
import com.ispengya.polyconfig.storage.snapshot.SnapshotEntry;
import com.ispengya.polyconfig.storage.snapshot.SnapshotHistory;
import com.ispengya.polyconfig.storage.snapshot.SnapshotHistoryItem;
import com.ispengya.polyconfig.storage.snapshot.SnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单机协议实现。
 *
 * <p>命令先并行到达、由组提交线程批量追加并统一刷盘，应答前保证已持久化；
 * 启动时优先加载快照再从快照位点回放增量。快照达到阈值后异步落盘，
 * 成功即删除被完整覆盖的日志前缀段。</p>
 */
public class LocalProtocol implements ConfigProtocol {

    private static final Logger log = LoggerFactory.getLogger(LocalProtocol.class);

    /**
     * 配置日志。
     */
    private final SegmentLog segmentLog;

    /**
     * 配置状态机。
     */
    private final ConfigStateMachine stateMachine;

    /**
     * 命名空间与分组元数据。
     */
    private final WorkspaceRegistry workspaceRegistry;

    /**
     * 快照存储。
     */
    private final SnapshotStore snapshotStore;

    /**
     * 快照触发阈值（命令条数）；0 表示关闭快照。
     */
    private final int snapshotThreshold;

    /**
     * 组提交待写队列。
     */
    private final BlockingQueue<PendingWrite> pendingWrites = new LinkedBlockingQueue<>();

    /**
     * 组提交线程运行标记。
     */
    private final AtomicBoolean batcherRunning = new AtomicBoolean(true);

    /**
     * 节点 ID。
     */
    private final String nodeId;

    /**
     * 集群 ID。
     */
    private final String clusterId;

    /**
     * 节点对外主机名或 IP。
     */
    private volatile String host = "";

    /**
     * HTTP 端口。
     */
    private volatile int httpPort;

    /**
     * 客户端长连接端口。
     */
    private volatile int remotingPort;

    /**
     * 已应用的命令序号。
     */
    private long revision;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * 存储层致命错误后进入 fail-stop：拒绝新写入，重启恢复。
     */
    private volatile boolean broken;

    /**
     * 上次快照以来的命令计数（propose 锁内访问）。
     */
    private long commandsSinceSnapshot;

    /**
     * 是否有快照正在写入（propose 锁内访问）。
     */
    private boolean snapshotInFlight;

    /**
     * 快照异步写线程。
     */
    private final ExecutorService snapshotExecutor;

    /**
     * 组提交线程。
     */
    private final Thread batcherThread;

    /**
     * 创建单机协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     */
    public LocalProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit) {
        this(dataDir, nodeId, clusterId, historyLimit, 0);
    }

    /**
     * 创建单机协议并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照触发阈值（命令条数），0 表示关闭快照
     */
    public LocalProtocol(Path dataDir, String nodeId, String clusterId, int historyLimit,
                         int snapshotThreshold) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.snapshotThreshold = Math.max(0, snapshotThreshold);
        this.stateMachine = new ConfigStateMachine(historyLimit);
        this.workspaceRegistry = new WorkspaceRegistry(dataDir.resolve("workspaces.json"));
        this.snapshotStore = new SnapshotStore(dataDir.resolve("snapshot"));

        long startNanos = System.nanoTime();
        SnapshotData snapshot = this.snapshotThreshold > 0 ? this.snapshotStore.loadLatest() : null;
        this.segmentLog = new SegmentLog(SegmentLogConfig.builder(dataDir.resolve("log"))
                .syncOnAppend(false)
                .allowNonZeroStart(snapshot != null)
                .build());

        long replayFrom = 0L;
        if (snapshot != null) {
            stateMachine.restore(toEntries(snapshot), toHistories(snapshot));
            this.revision = snapshot.getRevision();
            replayFrom = snapshot.getLogOffset();
            if (replayFrom > segmentLog.getMaxOffset()) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "快照位点超出日志有效范围，数据可能损坏: snapshotOffset=" + replayFrom
                                + ", maxOffset=" + segmentLog.getMaxOffset());
            }
        }
        final long from = replayFrom;
        final long[] replayed = {0L};
        this.segmentLog.scanFrom(from, (offset, payload) -> {
            ConfigCommand command = CommandCodec.decode(payload);
            revision++;
            stateMachine.apply(command, revision);
            replayed[0]++;
            return true;
        });
        if (segmentLog.getRecoveryTruncatedOffset() >= 0) {
            log.warn("配置日志在启动时发生截断, truncatedOffset={}, quarantinedFiles={}",
                    segmentLog.getRecoveryTruncatedOffset(),
                    segmentLog.getQuarantinedFiles());
        }
        log.info("启动恢复完成, snapshotRevision={}, replayed={}, revision={}, 耗时={}ms",
                snapshot == null ? -1 : snapshot.getRevision(), replayed[0], revision,
                (System.nanoTime() - startNanos) / 1_000_000);

        this.snapshotExecutor = this.snapshotThreshold > 0
                ? Executors.newSingleThreadExecutor(daemonFactory("poly-config-snapshot")) : null;
        this.batcherThread = new Thread(this::runBatcher, "poly-config-batcher");
        this.batcherThread.setDaemon(true);
        this.batcherThread.start();
    }

    /**
     * 更新节点对外地址。
     *
     * @param host 主机名或 IP
     * @param httpPort HTTP 端口
     * @param remotingPort 客户端长连接端口
     */
    public void setNodeAddress(String host, int httpPort, int remotingPort) {
        this.host = host == null ? "" : host;
        this.httpPort = httpPort;
        this.remotingPort = remotingPort;
    }

    @Override
    public void start() {
        log.info("LocalProtocol started, nodeId={}, clusterId={}", nodeId, clusterId);
    }

    @Override
    public MutationResult propose(ConfigCommand command) {
        ensureUsable();
        PendingWrite pending;
        synchronized (this) {
            ensureUsable();
            checkExpectedVersion(command);
            byte[] payload = CommandCodec.encode(command);
            revision++;
            stateMachine.apply(command, revision);
            pending = new PendingWrite(payload, revision, command);
            maybeTriggerSnapshotLocked(pending);
            // 入队必须在锁内：保证日志顺序与 revision 顺序一致，
            // 否则快照位点与回放会出现命令重复应用（revision 膨胀、历史重复）
            pendingWrites.add(pending);
        }
        // 等待刷盘在锁外进行，多个写者可并发等待、由组提交批量持久化
        return pending.await();
    }

    /**
     * 快照阈值命中时在 propose 锁内捕获状态并提交异步写。
     *
     * <p>捕获为 O(n) 浅拷贝（条目对象不可变）；序列化与落盘在快照线程。
     * 挂在本次命令的 {@code offsetFuture} 由组提交线程在刷盘完成后补齐，
     * 保证快照位点对应的日志已持久化。</p>
     */
    private void maybeTriggerSnapshotLocked(PendingWrite pending) {
        if (snapshotThreshold <= 0 || snapshotInFlight) {
            return;
        }
        commandsSinceSnapshot++;
        if (commandsSinceSnapshot < snapshotThreshold) {
            return;
        }
        CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        pending.offsetFuture = offsetFuture;
        SnapshotData data = captureLocked();
        long capturedRevision = revision;
        snapshotInFlight = true;
        snapshotExecutor.execute(() -> persistSnapshot(data, capturedRevision, offsetFuture));
    }

    private SnapshotData captureLocked() {
        SnapshotData data = new SnapshotData();
        data.setRevision(revision);
        List<SnapshotEntry> entries = new ArrayList<>();
        for (ConfigEntry entry : stateMachine.exportEntries()) {
            SnapshotEntry row = new SnapshotEntry();
            row.setNamespace(entry.getKey().getNamespace());
            row.setGroup(entry.getKey().getGroup());
            row.setDataId(entry.getKey().getDataId());
            row.setContent(entry.getContent());
            row.setType(entry.getType() == null ? null : entry.getType().name());
            row.setMd5(entry.getMd5());
            row.setVersion(entry.getVersion());
            row.setRevision(entry.getRevision());
            row.setCreatedTime(entry.getCreatedTime());
            row.setModifiedTime(entry.getModifiedTime());
            row.setOperator(entry.getOperator());
            entries.add(row);
        }
        data.setEntries(entries);
        List<SnapshotHistory> histories = new ArrayList<>();
        for (Map.Entry<ConfigKey, List<HistoryEntry>> entry
                : stateMachine.exportHistories().entrySet()) {
            SnapshotHistory history = new SnapshotHistory();
            history.setNamespace(entry.getKey().getNamespace());
            history.setGroup(entry.getKey().getGroup());
            history.setDataId(entry.getKey().getDataId());
            List<SnapshotHistoryItem> items = new ArrayList<>();
            for (HistoryEntry item : entry.getValue()) {
                SnapshotHistoryItem row = new SnapshotHistoryItem();
                row.setRevision(item.getRevision());
                row.setOperation(item.getOperation() == null ? null : item.getOperation().name());
                row.setContent(item.getContent());
                row.setType(item.getType() == null ? null : item.getType().name());
                row.setMd5(item.getMd5());
                row.setOperator(item.getOperator());
                row.setTimestamp(item.getTimestamp());
                items.add(row);
            }
            history.setItems(items);
            histories.add(history);
        }
        data.setHistories(histories);
        return data;
    }

    private void persistSnapshot(SnapshotData data, long capturedRevision,
                                 CompletableFuture<Long> offsetFuture) {
        try {
            long logOffset = offsetFuture.get(30, TimeUnit.SECONDS);
            data.setLogOffset(logOffset);
            snapshotStore.write(data);
            int deleted = segmentLog.deleteSegmentsBefore(logOffset);
            log.info("快照完成, revision={}, logOffset={}, 删除前缀段={}, entries={}",
                    capturedRevision, logOffset, deleted, data.getEntries().size());
            synchronized (this) {
                commandsSinceSnapshot = 0;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("快照写入被中断, revision={}", capturedRevision);
        } catch (ExecutionException | java.util.concurrent.TimeoutException | RuntimeException e) {
            log.warn("快照写入失败，稍后重试, revision={}", capturedRevision, e);
            synchronized (this) {
                // 失败后折半退避，避免每条命令都触发一次注定失败的快照
                commandsSinceSnapshot = commandsSinceSnapshot / 2;
            }
        } finally {
            synchronized (this) {
                snapshotInFlight = false;
            }
        }
    }

    /**
     * 组提交循环：批量追加，一轮一次刷盘，刷盘后统一应答。
     */
    private void runBatcher() {
        List<PendingWrite> batch = new ArrayList<>();
        while (batcherRunning.get() || !pendingWrites.isEmpty()) {
            batch.clear();
            PendingWrite first;
            try {
                first = pendingWrites.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (first == null) {
                continue;
            }
            batch.add(first);
            pendingWrites.drainTo(batch);
            try {
                for (PendingWrite pending : batch) {
                    segmentLog.append(pending.payload);
                    pending.logEndOffset = segmentLog.getMaxOffset();
                }
                segmentLog.flush();
                for (PendingWrite pending : batch) {
                    completeWrite(pending);
                }
            } catch (RuntimeException e) {
                log.error("组提交写入失败，协议进入 fail-stop，请重启恢复", e);
                broken = true;
                for (PendingWrite pending : batch) {
                    pending.done.completeExceptionally(new PolyConfigException(
                            ErrorCode.STORAGE_ERROR, "日志写入失败", e));
                }
            }
        }
    }

    private void completeWrite(PendingWrite pending) {
        if (pending.offsetFuture != null) {
            pending.offsetFuture.complete(pending.logEndOffset);
        }
        ConfigCommand command = pending.command;
        String md5 = command.getOperation() == ConfigOperation.DELETE
                ? "" : Md5Utils.md5Hex(command.getContent());
        pending.done.complete(new MutationResult(command.getKey(),
                pending.revision, pending.revision, md5));
    }

    @Override
    public Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency) {
        ensureOpen();
        return stateMachine.get(key);
    }

    @Override
    public List<ConfigEntry> list(String namespace, String group) {
        ensureOpen();
        return stateMachine.list(namespace, group);
    }

    @Override
    public List<HistoryEntry> history(ConfigKey key, int limit) {
        ensureOpen();
        return stateMachine.history(key, limit);
    }

    @Override
    public List<String> namespaces() {
        ensureOpen();
        Set<String> result = new TreeSet<>(workspaceRegistry.namespaces());
        result.addAll(stateMachine.namespaces());
        return new ArrayList<>(result);
    }

    @Override
    public List<String> groups(String namespace) {
        ensureOpen();
        Set<String> result = new TreeSet<>(workspaceRegistry.groups(namespace));
        result.addAll(stateMachine.groups(namespace));
        return new ArrayList<>(result);
    }

    @Override
    public void createNamespace(String namespace) {
        ensureOpen();
        workspaceRegistry.createNamespace(namespace);
    }

    @Override
    public void createGroup(String namespace, String group) {
        ensureOpen();
        workspaceRegistry.createGroup(namespace, group);
    }

    @Override
    public NodeStatus status() {
        return new NodeStatus(nodeId, host, httpPort, remotingPort,
                ProtocolMode.STANDALONE, NodeRole.STANDALONE, !closed, 0L, "", 1);
    }

    @Override
    public ClusterStatus clusterStatus() {
        return new ClusterStatus(clusterId, nodeId, ProtocolMode.STANDALONE,
                Collections.singletonList(status()));
    }

    @Override
    public void addChangeListener(ChangeListener listener) {
        stateMachine.addChangeListener(listener);
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        stateMachine.removeChangeListener(listener);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        batcherRunning.set(false);
        try {
            batcherThread.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("等待组提交线程退出被中断");
        }
        if (snapshotThreshold > 0 && !broken) {
            try {
                SnapshotData data = null;
                long logOffset = -1L;
                synchronized (this) {
                    if (commandsSinceSnapshot > 0) {
                        data = captureLocked();
                        // 组提交线程已退出，此时 maxOffset 之前的日志全部已刷盘
                        logOffset = segmentLog.getMaxOffset();
                    }
                }
                if (data != null) {
                    data.setLogOffset(logOffset);
                    snapshotStore.write(data);
                    segmentLog.deleteSegmentsBefore(logOffset);
                    log.info("关闭前最终快照完成, revision={}, logOffset={}", revision, logOffset);
                }
            } catch (RuntimeException e) {
                log.warn("关闭前最终快照失败", e);
            }
        }
        if (snapshotExecutor != null) {
            snapshotExecutor.shutdownNow();
        }
        segmentLog.close();
    }

    /**
     * 校验乐观锁期望版本。
     *
     * <p>在 propose 锁内执行，保证"检查-应用"原子；校验不通过抛
     * {@code VERSION_CONFLICT}，命令不会进入日志。回放路径不携带
     * expectedVersion，不会重复校验。</p>
     */
    private void checkExpectedVersion(ConfigCommand command) {
        Long expected = command.getExpectedVersion();
        if (expected == null) {
            return;
        }
        long current = stateMachine.get(command.getKey())
                .map(entry -> entry.getRevision())
                .orElse(-1L);
        if (expected.longValue() != current) {
            throw new PolyConfigException(ErrorCode.VERSION_CONFLICT,
                    "配置版本冲突: key=" + command.getKey()
                            + ", expected=" + expected + ", current=" + current);
        }
    }

    private void ensureUsable() {
        ensureOpen();
        if (broken) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "日志写入失败，协议已 fail-stop，请重启恢复");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "协议已关闭");
        }
    }

    private static List<ConfigEntry> toEntries(SnapshotData snapshot) {
        List<ConfigEntry> entries = new ArrayList<>();
        if (snapshot.getEntries() == null) {
            return entries;
        }
        for (SnapshotEntry row : snapshot.getEntries()) {
            ConfigKey key = new ConfigKey(row.getNamespace(), row.getGroup(), row.getDataId());
            entries.add(new ConfigEntry(key, row.getContent(),
                    ConfigType.valueOf(row.getType()), row.getMd5(),
                    row.getVersion(), row.getRevision(),
                    row.getCreatedTime(), row.getModifiedTime(), row.getOperator()));
        }
        return entries;
    }

    private static Map<ConfigKey, List<HistoryEntry>> toHistories(SnapshotData snapshot) {
        Map<ConfigKey, List<HistoryEntry>> result = new java.util.HashMap<>();
        if (snapshot.getHistories() == null) {
            return result;
        }
        for (SnapshotHistory history : snapshot.getHistories()) {
            ConfigKey key = new ConfigKey(history.getNamespace(), history.getGroup(),
                    history.getDataId());
            List<HistoryEntry> items = new ArrayList<>();
            if (history.getItems() != null) {
                for (SnapshotHistoryItem row : history.getItems()) {
                    items.add(new HistoryEntry(key, row.getRevision(),
                            ConfigOperation.valueOf(row.getOperation()), row.getContent(),
                            row.getType() == null ? null : ConfigType.valueOf(row.getType()),
                            row.getMd5(), row.getOperator(), row.getTimestamp()));
                }
            }
            result.put(key, items);
        }
        return result;
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger index = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * 一条待持久化命令：由组提交线程追加并刷盘后应答。
     */
    private static final class PendingWrite {

        private final byte[] payload;
        private final long revision;
        private final ConfigCommand command;
        private final CompletableFuture<MutationResult> done = new CompletableFuture<>();

        /**
         * 快照触发时挂在本命令上的日志位点 future，刷盘后补齐；未触发时为 null。
         */
        private volatile CompletableFuture<Long> offsetFuture;

        private volatile long logEndOffset = -1L;

        private PendingWrite(byte[] payload, long revision, ConfigCommand command) {
            this.payload = payload;
            this.revision = revision;
            this.command = command;
        }

        private MutationResult await() {
            try {
                return done.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof PolyConfigException) {
                    throw (PolyConfigException) cause;
                }
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志写入失败", cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PolyConfigException(ErrorCode.UNAVAILABLE, "等待写入持久化被中断");
            }
        }
    }
}
