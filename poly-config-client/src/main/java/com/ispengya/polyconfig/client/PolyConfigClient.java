package com.ispengya.polyconfig.client;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.FetchRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.PushEvent;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
        subscriptions.add(key);
        try {
            subscribe(key, false);
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
            subscriptions.remove(key);
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
        SubscribeRequest request = SubscribeRequest.of(key.getNamespace(), key.getGroup(),
                key.getDataId(), md5Cache.getOrDefault(key, ""));
        RemotingCommand response = invokeWithFailover(ProtocolCodes.SUBSCRIBE,
                JsonUtils.toJson(request).getBytes(StandardCharsets.UTF_8),
                options.getRequestTimeoutMillis());
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
            if ("DELETE".equals(event.getOperation())) {
                contentCache.remove(key);
                md5Cache.remove(key);
                deleteSnapshot(key);
                notifyListeners(key, null);
                return;
            }
            ConfigView view = fetch(key, options.getRequestTimeoutMillis());
            if (view == null) {
                contentCache.remove(key);
                md5Cache.remove(key);
                deleteSnapshot(key);
                notifyListeners(key, null);
                return;
            }
            applyView(view, true);
        } catch (PolyConfigException e) {
            log.warn("推送后拉取配置失败，等待定时对账: {}", key, e);
        } catch (RuntimeException e) {
            log.warn("处理配置推送失败: {}", key, e);
        }
    }

    private void applyView(ConfigView view, boolean notify) {
        if (view == null) {
            return;
        }
        ConfigKey key = new ConfigKey(view.getNamespace(), view.getGroup(), view.getDataId());
        String content = view.getContent() == null ? "" : view.getContent();
        String previous = contentCache.put(key, content);
        md5Cache.put(key, view.getMd5());
        writeSnapshot(key, content);
        if (notify && (previous == null || !previous.equals(content))) {
            notifyListeners(key, content);
        }
    }

    private void notifyListeners(ConfigKey key, String content) {
        CopyOnWriteArrayList<ConfigListener> list = listeners.get(key);
        if (list == null || list.isEmpty()) {
            return;
        }
        for (ConfigListener listener : list) {
            try {
                callbackExecutor.execute(() -> {
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

    private void resyncSafely() {
        if (closed.get()) {
            return;
        }
        for (ConfigKey key : subscriptions) {
            if (closed.get()) {
                return;
            }
            try {
                subscribe(key, true);
            } catch (PolyConfigException e) {
                log.debug("定时对账失败: {}", key, e);
            } catch (RuntimeException e) {
                log.warn("定时对账异常: {}", key, e);
            }
        }
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
