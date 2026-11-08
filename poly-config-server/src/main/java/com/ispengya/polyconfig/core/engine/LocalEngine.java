package com.ispengya.polyconfig.core.engine;

import com.ispengya.polyconfig.common.codec.CommandCodec;
import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigConstants;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.VerifyEntry;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.core.state.ConfigStateMachine;
import com.ispengya.polyconfig.core.workspace.WorkspaceRegistry;
import com.ispengya.polyconfig.storage.log.SegmentLog;
import com.ispengya.polyconfig.storage.log.SegmentLogConfig;
import com.ispengya.polyconfig.storage.snapshot.SnapshotData;
import com.ispengya.polyconfig.storage.snapshot.SnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
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
 * 本地存储引擎：分段日志 + 状态机 + 组提交 + 快照。
 *
 * <p>standalone 与 AP 的 owner/副本路径共用同一个引擎，两个写入口：</p>
 * <ul>
 *   <li>{@link #proposeLocal(ConfigCommand)}：本地路径——校验 expectedVersion、
 *       分配版本（version = revision）、落盘后应答；</li>
 *   <li>{@link #applyReplicated(ConfigCommand)}：副本路径——使用命令携带的
 *       owner 分配版本（{@code assignedVersion}）apply，仍落本地日志，
 *       重启不依赖 peers 在线。</li>
 * </ul>
 *
 * <p>命令先并行到达、由组提交线程批量追加并统一刷盘，应答前保证已持久化；
 * 启动时优先加载快照再从快照位点回放增量。</p>
 */
public class LocalEngine {

    private static final Logger log = LoggerFactory.getLogger(LocalEngine.class);

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
     * 节点 ID（日志用）。
     */
    private final String nodeId;

    /**
     * 集群 ID（日志用）。
     */
    private final String clusterId;

    /**
     * 已应用的命令序号（节点本地位点）。
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
     * 本地写成功后的回调；AP 模式下用于 owner 复制入队，standalone 不设置。
     */
    private volatile ProposeListener proposeListener;

    /**
     * 上次快照以来的命令计数（写锁内访问）。
     */
    private long commandsSinceSnapshot;

    /**
     * 是否有快照正在写入（写锁内访问）。
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
     * 创建引擎并恢复状态。
     *
     * @param dataDir 数据目录
     * @param nodeId 节点 ID
     * @param clusterId 集群 ID
     * @param historyLimit 历史保留条数
     * @param snapshotThreshold 快照触发阈值（命令条数），0 表示关闭快照
     */
    public LocalEngine(Path dataDir, String nodeId, String clusterId, int historyLimit,
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
            Long assigned = command.getAssignedVersion();
            if (assigned != null) {
                // 副本路径回放：使用 owner 分配的版本，与持久化时一致
                stateMachine.apply(command, revision, assigned);
            } else {
                stateMachine.apply(command, revision, revision);
            }
            replayed[0]++;
            return true;
        });
        if (segmentLog.getRecoveryTruncatedOffset() >= 0) {
            log.warn("配置日志在启动时发生截断, truncatedOffset={}, quarantinedFiles={}",
                    segmentLog.getRecoveryTruncatedOffset(),
                    segmentLog.getQuarantinedFiles());
        }
        log.info("启动恢复完成, nodeId={}, snapshotRevision={}, replayed={}, revision={}, 耗时={}ms",
                nodeId, snapshot == null ? -1 : snapshot.getRevision(), replayed[0], revision,
                (System.nanoTime() - startNanos) / 1_000_000);

        this.snapshotExecutor = this.snapshotThreshold > 0
                ? Executors.newSingleThreadExecutor(daemonFactory("poly-config-snapshot")) : null;
        this.batcherThread = new Thread(this::runBatcher, "poly-config-batcher");
        this.batcherThread.setDaemon(true);
        this.batcherThread.start();
    }

    /**
     * 本地写入口：校验 expectedVersion、分配版本、落盘并应答。
     *
     * <p>在锁内完成版本校验、内存 apply 与锁内入队（保证日志顺序 = revision 顺序），
     * 随后并发等待组提交刷盘——应答前必持久化。</p>
     *
     * @param command 配置命令
     * @return 提交结果
     */
    public MutationResult proposeLocal(ConfigCommand command) {
        ensureUsable();
        PendingWrite pending;
        synchronized (this) {
            ensureUsable();
            checkExpectedVersion(command);
            byte[] payload = CommandCodec.encode(command);
            revision++;
            stateMachine.apply(command, revision, revision);
            pending = new PendingWrite(payload, revision, command);
            maybeTriggerSnapshotLocked(pending);
            pendingWrites.add(pending);
        }
        MutationResult result = pending.await();
        // 刷盘成功后回调（owner 复制入队点）；仅本地分配路径，副本 apply 不触发
        ProposeListener listener = this.proposeListener;
        if (listener != null) {
            try {
                listener.onProposed(command, result);
            } catch (RuntimeException e) {
                log.warn("本地写回调执行失败, key={}", command.getKey(), e);
            }
        }
        return result;
    }

    /**
     * 设置本地写成功后的回调（owner 复制入队用）。
     *
     * @param listener 回调；{@code null} 表示移除
     */
    public void setProposeListener(ProposeListener listener) {
        this.proposeListener = listener;
    }

    /**
     * 副本写入口：应用携带 owner 分配版本的复制命令。
     *
     * <p>不校验 expectedVersion（owner 已校验过），版本用命令携带的
     * {@code assignedVersion}；同样落本地日志并等待刷盘——转发节点已应答
     * 客户端的写在本节点也必须持久化。调用方负责命令幂等（旧版本丢弃）。</p>
     *
     * @param command 携带 assignedVersion 的配置命令
     * @return 提交结果（version 为 owner 分配值）
     */
    public MutationResult applyReplicated(ConfigCommand command) {
        ensureUsable();
        Long assigned = command.getAssignedVersion();
        if (assigned == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "副本命令缺少 assignedVersion: " + command.getKey());
        }
        PendingWrite pending;
        synchronized (this) {
            ensureUsable();
            byte[] payload = CommandCodec.encode(command);
            revision++;
            stateMachine.apply(command, revision, assigned);
            pending = new PendingWrite(payload, revision, command);
            maybeTriggerSnapshotLocked(pending);
            pendingWrites.add(pending);
        }
        return pending.await();
    }

    /**
     * 副本侧幂等应用：比本地新才应用，否则丢弃（docs/ap-design.md §6）。
     *
     * <p>规则与 {@code DISTRO_SYNC} 批次一致：本地已有且 version 不小于
     * 来值则丢弃（重复、乱序无害），本地没有且是删除也丢弃；否则走
     * {@link #applyReplicated(ConfigCommand)} 落盘并触发本地推送。
     * 复制批次与周期校验增量共用本入口。</p>
     *
     * @param record 命令记录（须携带 owner 分配的 version）
     * @return 是否实际应用
     */
    public boolean applyReplicatedIfNewer(CommandRecord record) {
        ensureOpen();
        ConfigCommand command = record.toCommand();
        Long version = command.getAssignedVersion();
        if (version == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "复制命令缺少 owner 分配版本: " + command.getKey());
        }
        Optional<ConfigEntry> local = stateMachine.get(command.getKey());
        if (local.isPresent() && local.get().getVersion() >= version.longValue()) {
            return false;
        }
        if (!local.isPresent() && command.getOperation() == ConfigOperation.DELETE) {
            // 已删或从未存在：删除不改变现状，跳过保持幂等
            return false;
        }
        applyReplicated(command);
        return true;
    }

    /**
     * 导出周期校验摘要表（活条目 + 墓碑），供校验器发送。
     *
     * @return 摘要条目列表
     */
    public List<VerifyEntry> verifyTable() {
        ensureOpen();
        return stateMachine.verifyTable();
    }

    /**
     * 当前节点本地变更序号（日志/快照位点，不跨节点比较）。
     *
     * @return 本地 revision
     */
    public long currentRevision() {
        synchronized (this) {
            return revision;
        }
    }

    public Optional<ConfigEntry> read(ConfigKey key) {
        ensureOpen();
        return stateMachine.get(key);
    }

    public List<ConfigEntry> list(String namespace, String group) {
        ensureOpen();
        return stateMachine.list(namespace, group);
    }

    public List<HistoryEntry> history(ConfigKey key, int limit) {
        ensureOpen();
        return stateMachine.history(key, limit);
    }

    public List<String> namespaces() {
        ensureOpen();
        Set<String> result = new TreeSet<>(workspaceRegistry.namespaces());
        result.addAll(stateMachine.namespaces());
        // AP 内置分区里的命名空间标记条目（dataId 即命名空间名）
        for (ConfigEntry entry : stateMachine.list(ConfigConstants.SYSTEM_NAMESPACE,
                ConfigConstants.WORKSPACE_GROUP)) {
            result.add(entry.getKey().getDataId());
        }
        // 内建系统命名空间不对外展示（standalone 不存在该条目，过滤为空操作）
        result.remove(ConfigConstants.SYSTEM_NAMESPACE);
        return new ArrayList<>(result);
    }

    public List<String> groups(String namespace) {
        ensureOpen();
        String target = namespace == null || namespace.trim().isEmpty()
                ? ConfigConstants.DEFAULT_NAMESPACE : namespace.trim();
        Set<String> result = new TreeSet<>(workspaceRegistry.groups(target));
        result.addAll(stateMachine.groups(target));
        // AP 标记条目：(命名空间, _workspaces, 分组名)
        for (ConfigEntry entry : stateMachine.list(target, ConfigConstants.WORKSPACE_GROUP)) {
            result.add(entry.getKey().getDataId());
        }
        // 内建标记分组不对外展示
        result.remove(ConfigConstants.WORKSPACE_GROUP);
        return new ArrayList<>(result);
    }

    public void createNamespace(String namespace) {
        ensureOpen();
        workspaceRegistry.createNamespace(namespace);
    }

    public void createGroup(String namespace, String group) {
        ensureOpen();
        workspaceRegistry.createGroup(namespace, group);
    }

    public void addChangeListener(ChangeListener listener) {
        stateMachine.addChangeListener(listener);
    }

    public void removeChangeListener(ChangeListener listener) {
        stateMachine.removeChangeListener(listener);
    }

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
     * 快照阈值命中时在写锁内捕获状态并提交异步写。
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
        // 快照捕获统一走状态机导出（CP 同一套零件，lastIncluded 由 CP 填写）
        return stateMachine.exportSnapshot(revision, 0L, 0L);
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
                log.error("组提交写入失败，引擎进入 fail-stop，请重启恢复", e);
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
        Long assigned = command.getAssignedVersion();
        long version = assigned != null ? assigned : pending.revision;
        String md5 = command.getOperation() == ConfigOperation.DELETE
                ? "" : Md5Utils.md5Hex(command.getContent());
        pending.done.complete(new MutationResult(command.getKey(),
                version, pending.revision, md5));
    }

    /**
     * 校验乐观锁期望版本（与跨节点权威的 version 比较）。
     *
     * <p>在写锁内执行，保证"检查-应用"原子；校验不通过抛
     * {@code VERSION_CONFLICT}，命令不会进入日志。回放路径不携带
     * expectedVersion，不会重复校验。</p>
     */
    private void checkExpectedVersion(ConfigCommand command) {
        Long expected = command.getExpectedVersion();
        if (expected == null) {
            return;
        }
        long current = stateMachine.get(command.getKey())
                .map(entry -> entry.getVersion())
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
                    "日志写入失败，引擎已 fail-stop，请重启恢复");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "引擎已关闭");
        }
    }

    private static List<ConfigEntry> toEntries(SnapshotData snapshot) {
        return snapshot.toEntries();
    }

    private static Map<ConfigKey, List<HistoryEntry>> toHistories(SnapshotData snapshot) {
        return snapshot.toHistories();
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
     * 本地写成功（已持久化）后的回调。
     *
     * <p>AP 模式由 {@code DistroProtocol} 注册，用于把 owner 分配的命令
     * 入复制队列；仅 {@link #proposeLocal} 触发，副本 {@link #applyReplicated}
     * 不触发——复制只从 owner 发起。</p>
     */
    public interface ProposeListener {

        /**
         * 本地写已提交并刷盘。
         *
         * @param command 已提交的配置命令
         * @param result 提交结果（version 为本节点分配值）
         */
        void onProposed(ConfigCommand command, MutationResult result);
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
