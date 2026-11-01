package com.ispengya.polyconfig.client;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeRequest;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeResponse;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.FetchRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.PushEvent;
import com.ispengya.polyconfig.common.protocol.SubscribeItem;
import com.ispengya.polyconfig.common.protocol.SubscribeRequest;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * poly-config Java 客户端。
 *
 * <p>订阅走长连接推送，内容按需拉取；本地维护内容缓存、MD5 和快照文件，
 * 服务端不可用时仍可读取最后一次成功同步的配置。</p>
 */
public class PolyConfigClient implements ConfigService {

    private static final Logger log = LoggerFactory.getLogger(PolyConfigClient.class);

    /**
     * 客户端配置。
     */
    private final ClientOptions options;

    /**
     * 长连接客户端。
     */
    private final NettyRemotingClient remotingClient;

    /**
     * 配置内容缓存。
     */
    private final ConcurrentHashMap<ConfigKey, String> contentCache = new ConcurrentHashMap<>();

    /**
     * 配置 MD5 缓存。
     */
    private final ConcurrentHashMap<ConfigKey, String> md5Cache = new ConcurrentHashMap<>();

    /**
     * 每个配置键最后应用的 version（跨节点权威序号），用于丢弃乱序到达的旧视图（ISSUE-02）。
     *
     * <p>AP 模式下 revision 是节点本地序号、跨节点不可比，单调守卫以 owner 分配的
     * version 为准；推送、拉取、对账共用此规则。</p>
     */
    private final ConcurrentHashMap<ConfigKey, Long> versionCache = new ConcurrentHashMap<>();

    /**
     * 每个配置键的应用锁，序列化缓存更新与通知提交。
     */
    private final ConcurrentHashMap<ConfigKey, Object> keyLocks = new ConcurrentHashMap<>();

    /**
     * 每个配置键的串行回调队列，保证同一 key 的回调按提交顺序执行（ISSUE-03）。
     */
    private final ConcurrentHashMap<ConfigKey, SerialExecutor> callbackQueues =
            new ConcurrentHashMap<>();

    /**
     * 监听器表。
     */
    private final ConcurrentHashMap<ConfigKey, CopyOnWriteArrayList<ConfigListener>> listeners =
            new ConcurrentHashMap<>();

    /**
     * 已订阅的配置键。
     */
    private final Set<ConfigKey> subscriptions = ConcurrentHashMap.newKeySet();

    /**
     * 回调线程池。
     */
    private final ExecutorService callbackExecutor;

    /**
     * 定时对账线程。
     */
    private final ScheduledExecutorService resyncScheduler;

    /**
     * 当前服务端下标，用于故障转移。
     */
    private final AtomicInteger currentServerIndex = new AtomicInteger();

    /**
     * 是否已关闭。
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 单次批量对账的最大键数，与服务端上限对齐。
     */
    private static final int RESYNC_BATCH_SIZE = 512;

    /**
     * 初始订阅的短超时：连接被拒立即失败，黑洞场景阻塞上界。
     */
    private static final long INITIAL_SUBSCRIBE_TIMEOUT_MILLIS = 200L;

    /**
     * 连续对账失败次数，用于指数退避。
     */
    private final AtomicInteger consecutiveResyncFailures = new AtomicInteger();

    /**
     * 下次允许对账的时间点，退避期内直接跳过。
     */
    private volatile long nextResyncTimeMillis;

    /**
     * 创建客户端并启动长连接。
     *
     * @param options 客户端配置
     */
    public PolyConfigClient(ClientOptions options) {
        if (options == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "options 不能为空");
        }
        this.options = options;
        this.remotingClient = new NettyRemotingClient();
        this.remotingClient.start();
        this.remotingClient.registerPushListener(
                ProtocolCodes.PUSH_CONFIG_CHANGED, this::handlePush);
        this.callbackExecutor = Executors.newFixedThreadPool(4,
                daemonFactory("poly-config-client-callback"));
        this.resyncScheduler = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("poly-config-client-resync"));
        this.resyncScheduler.scheduleWithFixedDelay(this::resyncSafely,
                options.getResyncIntervalMillis(), options.getResyncIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public String getConfig(String namespace, String group, String dataId, long timeoutMillis) {
        ensureOpen();
        ConfigKey key = key(namespace, group, dataId);
        String cached = contentCache.get(key);
        if (cached != null) {
            return cached;
        }
        try {
            ConfigView view = fetch(key, timeout(timeoutMillis));
            if (view == null) {
                return null;
            }
            applyView(view, false);
            return view.getContent();
        } catch (PolyConfigException e) {
            String snapshot = readSnapshot(key);
            if (snapshot != null) {
                contentCache.put(key, snapshot);
                md5Cache.put(key, Md5Utils.md5Hex(snapshot));
                log.warn("服务端不可用，使用本地快照: {}", key);
                return snapshot;
            }
            throw e;
        }
    }

    @Override
    public void addListener(String namespace, String group, String dataId,
                            ConfigListener listener) {
        ensureOpen();
        if (listener == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "listener 不能为空");
        }
        ConfigKey key = key(namespace, group, dataId);
        listeners.computeIfAbsent(key, value -> new CopyOnWriteArrayList<>())
                .addIfAbsent(listener);
        if (!subscriptions.add(key)) {
            // 已订阅过该 key，服务端订阅关系已存在，无需重复初始订阅
            return;
        }
        // 先读本地快照，保证注册后立即可读
        if (!contentCache.containsKey(key)) {
            String snapshot = readSnapshot(key);
            if (snapshot != null) {
                contentCache.putIfAbsent(key, snapshot);
                md5Cache.putIfAbsent(key, Md5Utils.md5Hex(snapshot));
                log.debug("使用本地快照初始化缓存: {}", key);
            }
        }
        // 初始订阅用短超时有界等待：服务端可用时注册返回即订阅生效（随后的变更推送不丢），
        // 拒绝连接立即失败，网络黑洞最多阻塞 INITIAL_SUBSCRIBE_TIMEOUT_MILLIS；
        // 失败不抛出，由定时对账补救
        try {
            subscribe(key, false, INITIAL_SUBSCRIBE_TIMEOUT_MILLIS);
        } catch (PolyConfigException e) {
            log.warn("初始订阅失败，等待定时对账: {}", key, e);
        }
    }

    @Override
    public void removeListener(String namespace, String group, String dataId,
                               ConfigListener listener) {
        ConfigKey key = key(namespace, group, dataId);
        CopyOnWriteArrayList<ConfigListener> list = listeners.get(key);
        if (list == null) {
            return;
        }
        list.remove(listener);
        if (list.isEmpty()) {
            listeners.remove(key, list);
            if (subscriptions.remove(key)) {
                sendUnsubscribeAsync(key);
            }
        }
    }

    /**
     * 异步通知服务端取消订阅，失败只记录日志——连接断开时服务端会自行清理。
     *
     * @param key 配置键
     */
    private void sendUnsubscribeAsync(ConfigKey key) {
        try {
            resyncScheduler.execute(() -> {
                if (closed.get()) {
                    return;
                }
                try {
                    SubscribeRequest request = SubscribeRequest.of(key.getNamespace(),
                            key.getGroup(), key.getDataId(), "", 0L);
                    invokeWithFailover(ProtocolCodes.UNSUBSCRIBE,
                            JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8),
                            options.getRequestTimeoutMillis());
                } catch (RuntimeException e) {
                    log.debug("取消订阅失败(忽略): {}", key, e);
                }
            });
        } catch (RejectedExecutionException e) {
            log.debug("客户端已关闭，忽略取消订阅: {}", key);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        resyncScheduler.shutdownNow();
        callbackExecutor.shutdownNow();
        remotingClient.shutdown();
    }

    private void subscribe(ConfigKey key, boolean notifyOnChange) {
        subscribe(key, notifyOnChange, options.getRequestTimeoutMillis());
    }

    /**
     * 执行单键订阅。
     *
     * @param key 配置键
     * @param notifyOnChange 内容变化时是否通知监听器
     * @param timeoutMillis 请求超时
     */
    private void subscribe(ConfigKey key, boolean notifyOnChange, long timeoutMillis) {
        SubscribeRequest request = SubscribeRequest.of(key.getNamespace(), key.getGroup(),
                key.getDataId(), md5Cache.getOrDefault(key, ""),
                versionCache.getOrDefault(key, 0L));
        RemotingCommand response = invokeWithFailover(ProtocolCodes.SUBSCRIBE,
                JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8), timeoutMillis);
        if (response.getCode() == ProtocolCodes.RESPONSE_NOT_FOUND) {
            // 本地持有内容但服务端已删除（首次订阅时服务端无此键仍返回 OK）
            if (contentCache.containsKey(key)) {
                applyDeletion(key, 0L);
            }
            return;
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "订阅失败: " + response.getRemark());
        }
        if (response.getBody() != null && response.getBody().length > 0) {
            ConfigView view = JsonUtils.parseObject(
                    new String(response.getBody(), StandardCharsets.UTF_8), ConfigView.class);
            if (view != null) {
                applyView(view, notifyOnChange);
            }
        }
    }

    private ConfigView fetch(ConfigKey key, long timeoutMillis) {
        FetchRequest request = FetchRequest.of(key.getNamespace(), key.getGroup(),
                key.getDataId());
        RemotingCommand response = invokeWithFailover(ProtocolCodes.FETCH,
                JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8), timeoutMillis);
        if (response.getCode() == ProtocolCodes.RESPONSE_NOT_FOUND) {
            return null;
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "拉取配置失败: " + response.getRemark());
        }
        if (response.getBody() == null || response.getBody().length == 0) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "拉取配置响应为空");
        }
        return JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8), ConfigView.class);
    }

    private RemotingCommand invokeWithFailover(int code, byte[] body, long timeoutMillis) {
        List<String> servers = options.getServers();
        int start = Math.floorMod(currentServerIndex.get(), servers.size());
        Exception lastError = null;
        for (int i = 0; i < servers.size(); i++) {
            int index = (start + i) % servers.size();
            String address = servers.get(index);
            RemotingCommand request = RemotingCommand.createRequestCommand(code, null);
            request.setBody(body);
            try {
                RemotingCommand response = remotingClient.invokeSync(
                        address, request, timeoutMillis);
                currentServerIndex.set(index);
                return response;
            } catch (Exception e) {
                lastError = e;
                log.warn("请求服务端失败, address={}, code={}, reason={}",
                        address, code, e.getMessage());
                log.debug("请求服务端失败详情", e);
            }
        }
        throw new PolyConfigException(ErrorCode.UNAVAILABLE, "所有服务端请求失败", lastError);
    }

    private void handlePush(RemotingCommand message) {
        if (closed.get() || message.getBody() == null || message.getBody().length == 0) {
            return;
        }
        final PushEvent event;
        try {
            event = JsonUtils.parseObject(
                    new String(message.getBody(), StandardCharsets.UTF_8), PushEvent.class);
        } catch (RuntimeException e) {
            log.warn("解析配置推送失败", e);
            return;
        }
        if (event == null || event.getDataId() == null) {
            return;
        }
        ConfigKey key = new ConfigKey(event.getNamespace(), event.getGroup(),
                event.getDataId());
        if (event.getMd5() != null && !event.getMd5().isEmpty()
                && event.getMd5().equals(md5Cache.get(key))) {
            return;
        }
        try {
            callbackExecutor.execute(() -> processPush(key, event));
        } catch (RejectedExecutionException e) {
            log.debug("客户端已关闭，忽略推送: {}", key);
        }
    }

    private void processPush(ConfigKey key, PushEvent event) {
        try {
            // 已取消订阅的 key 不再处理推送
            if (!subscriptions.contains(key)) {
                log.debug("忽略已取消订阅的推送: {}", key);
                return;
            }
            if ("DELETE".equals(event.getOperation())) {
                applyDeletion(key, event.getVersion());
                return;
            }
            ConfigView view = fetch(key, options.getRequestTimeoutMillis());
            if (view == null) {
                applyDeletion(key, event.getVersion());
                return;
            }
            applyView(view, true);
        } catch (PolyConfigException e) {
            log.warn("推送后拉取配置失败，等待定时对账: {}", key, e);
        } catch (RuntimeException e) {
            log.warn("处理配置推送失败: {}", key, e);
        }
    }

    /**
     * 应用配置视图。
     *
     * <p>version 小于已应用版本的旧视图直接丢弃（ISSUE-02）；同一 key 的缓存更新与
     * 通知提交在键锁内完成，保证通知提交顺序与 version 一致（ISSUE-03）。</p>
     *
     * @param view 配置视图
     * @param notify 内容变化时是否通知监听器
     * @return 是否实际应用
     */
    boolean applyView(ConfigView view, boolean notify) {
        if (view == null) {
            return false;
        }
        ConfigKey key = new ConfigKey(view.getNamespace(), view.getGroup(), view.getDataId());
        String content = view.getContent() == null ? "" : view.getContent();
        long version = view.getVersion();
        synchronized (lockFor(key)) {
            long last = versionCache.getOrDefault(key, -1L);
            if (version < last) {
                log.debug("丢弃过期配置视图, key={}, version={}, lastVersion={}",
                        key, version, last);
                return false;
            }
            versionCache.put(key, version);
            String previous = contentCache.put(key, content);
            md5Cache.put(key, view.getMd5());
            writeSnapshot(key, content);
            if (notify && (previous == null || !previous.equals(content))) {
                enqueueNotify(key, content);
            }
        }
        return true;
    }

    /**
     * 应用删除（推送删除或拉取不到内容）。
     *
     * <p>携带 version 时按单调守卫丢弃过期删除；version 未知（小于等于 0）时
     * 按旧行为直接删除。</p>
     *
     * @param key 配置键
     * @param version 删除事件的 version，未知时传 0
     */
    void applyDeletion(ConfigKey key, long version) {
        synchronized (lockFor(key)) {
            long last = versionCache.getOrDefault(key, -1L);
            if (version > 0 && version < last) {
                log.debug("丢弃过期删除, key={}, version={}, lastVersion={}",
                        key, version, last);
                return;
            }
            if (version > 0) {
                versionCache.put(key, version);
            }
            contentCache.remove(key);
            md5Cache.remove(key);
            enqueueNotify(key, null);
        }
        deleteSnapshot(key);
    }

    private Object lockFor(ConfigKey key) {
        return keyLocks.computeIfAbsent(key, value -> new Object());
    }

    private void enqueueNotify(ConfigKey key, String content) {
        CopyOnWriteArrayList<ConfigListener> list = listeners.get(key);
        if (list == null || list.isEmpty()) {
            return;
        }
        SerialExecutor queue = callbackQueues.computeIfAbsent(key,
                value -> new SerialExecutor(callbackExecutor));
        for (ConfigListener listener : list) {
            try {
                queue.execute(() -> {
                    if (closed.get()) {
                        return;
                    }
                    try {
                        listener.onChange(key.getDataId(), key.getGroup(), content);
                    } catch (RuntimeException e) {
                        log.warn("配置监听器执行失败: {}", key, e);
                    }
                });
            } catch (RejectedExecutionException e) {
                return;
            }
        }
    }

    /**
     * 串行执行器：把任务按提交顺序转交共享线程池逐个执行。
     *
     * <p>保证同一配置键的回调不并发、执行顺序与提交顺序一致；不同 key 之间仍然并行。</p>
     */
    private static final class SerialExecutor implements Executor {

        private final Executor delegate;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private Runnable active;

        SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void execute(Runnable command) {
            queue.add(() -> {
                try {
                    command.run();
                } finally {
                    scheduleNext();
                }
            });
            if (active == null) {
                scheduleNext();
            }
        }

        private synchronized void scheduleNext() {
            active = queue.poll();
            if (active != null) {
                delegate.execute(active);
            }
        }
    }

    /**
     * 定时对账：按批携带每个键的 md5/revision 一次往返拉取差异。
     *
     * <p>整轮失败（所有服务端不可达）时短路退出并按连续失败次数指数退避，
     * 避免服务端故障期间对每个键重复重试所有节点。</p>
     */
    private void resyncSafely() {
        if (closed.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now < nextResyncTimeMillis) {
            log.debug("对账退避中，跳过本轮: 还剩 {}ms", nextResyncTimeMillis - now);
            return;
        }
        List<ConfigKey> keys = new ArrayList<>(subscriptions);
        if (keys.isEmpty()) {
            return;
        }
        try {
            for (int from = 0; from < keys.size(); from += RESYNC_BATCH_SIZE) {
                if (closed.get()) {
                    return;
                }
                List<SubscribeItem> targets = new ArrayList<>();
                int to = Math.min(from + RESYNC_BATCH_SIZE, keys.size());
                for (int i = from; i < to; i++) {
                    ConfigKey key = keys.get(i);
                    targets.add(SubscribeItem.of(key.getNamespace(), key.getGroup(),
                            key.getDataId(), md5Cache.getOrDefault(key, ""),
                            versionCache.getOrDefault(key, 0L)));
                }
                resyncBatch(targets);
            }
            consecutiveResyncFailures.set(0);
            nextResyncTimeMillis = 0L;
        } catch (PolyConfigException e) {
            onResyncFailure(e);
        } catch (RuntimeException e) {
            log.warn("定时对账异常", e);
            onResyncFailure(e);
        }
    }

    /**
     * 执行一批对账并应用差异。
     *
     * @param targets 订阅条目
     */
    private void resyncBatch(List<SubscribeItem> targets) {
        BatchSubscribeRequest request = BatchSubscribeRequest.of(targets);
        RemotingCommand response = invokeWithFailover(ProtocolCodes.BATCH_SUBSCRIBE,
                JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8),
                options.getRequestTimeoutMillis());
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "批量对账失败: " + response.getRemark());
        }
        if (response.getBody() == null || response.getBody().length == 0) {
            return;
        }
        BatchSubscribeResponse diff = JsonUtils.parseObject(
                new String(response.getBody(), StandardCharsets.UTF_8),
                BatchSubscribeResponse.class);
        if (diff == null) {
            return;
        }
        if (diff.getChanged() != null) {
            for (ConfigView view : diff.getChanged()) {
                if (view == null || view.getDataId() == null) {
                    continue;
                }
                applyView(view, true);
            }
        }
        if (diff.getDeleted() != null) {
            for (SubscribeItem item : diff.getDeleted()) {
                if (item == null || item.getDataId() == null) {
                    continue;
                }
                applyDeletion(new ConfigKey(item.getNamespace(), item.getGroup(),
                        item.getDataId()), 0L);
            }
        }
    }

    /**
     * 对账失败处理：短路本轮并指数退避（1/2/4/8 个对账周期封顶）。
     *
     * @param cause 失败原因
     */
    private void onResyncFailure(Exception cause) {
        int failures = consecutiveResyncFailures.incrementAndGet();
        long backoffMillis = options.getResyncIntervalMillis()
                * (1L << Math.min(failures - 1, 3));
        nextResyncTimeMillis = System.currentTimeMillis() + backoffMillis;
        log.warn("定时对账失败，连续第 {} 次，退避 {}ms: {}",
                failures, backoffMillis, cause.getMessage());
    }

    private ConfigKey key(String namespace, String group, String dataId) {
        String targetNamespace = namespace == null || namespace.trim().isEmpty()
                ? options.getNamespace() : namespace;
        return new ConfigKey(targetNamespace, group, dataId);
    }

    private long timeout(long timeoutMillis) {
        return timeoutMillis > 0 ? timeoutMillis : options.getRequestTimeoutMillis();
    }

    private Path snapshotFile(ConfigKey key) {
        return options.getSnapshotDir()
                .resolve(encode(key.getNamespace()))
                .resolve(encode(key.getGroup()))
                .resolve(encode(key.getDataId()));
    }

    private void writeSnapshot(ConfigKey key, String content) {
        Path file = snapshotFile(key);
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            Files.write(temp, content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            log.warn("写入本地快照失败: {}", file, e);
        }
    }

    private String readSnapshot(ConfigKey key) {
        Path file = snapshotFile(key);
        if (!Files.isRegularFile(file)) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("读取本地快照失败: {}", file, e);
            return null;
        }
    }

    private void deleteSnapshot(ConfigKey key) {
        try {
            Files.deleteIfExists(snapshotFile(key));
        } catch (IOException e) {
            log.warn("删除本地快照失败: {}", key, e);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "客户端已关闭");
        }
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 not supported", e);
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger index = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
