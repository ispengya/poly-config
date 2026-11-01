package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.NetworkUtils;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.core.distro.DistroProtocol;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.polyconfig.server.http.ApiHandler;
import com.ispengya.polyconfig.server.http.StaticFileHandler;
import com.ispengya.polyconfig.server.push.ChangePusher;
import com.ispengya.polyconfig.server.push.SubscriptionRegistry;
import com.ispengya.polyconfig.server.remoting.BatchSubscribeProcessor;
import com.ispengya.polyconfig.server.remoting.FetchProcessor;
import com.ispengya.polyconfig.server.remoting.SubscribeProcessor;
import com.ispengya.polyconfig.server.remoting.UnsubscribeProcessor;
import com.ispengya.polyconfig.core.standalone.LocalProtocol;
import com.ispengya.remoting.netty.NettyRemotingServer;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * poly-config 服务端。
 *
 * <p>按 {@code --mode} 装配一致性协议：standalone 为 {@code LocalProtocol}，
 * ap 为 {@code DistroProtocol}（M2），cp 留给 Raft（M1）。HTTP 管理面、
 * 客户端长连接和控制台静态资源都在同一个进程内。</p>
 */
public class PolyConfigServer {

    private static final Logger log = LoggerFactory.getLogger(PolyConfigServer.class);

    /**
     * 启动配置。
     */
    private final ServerOptions options;

    /**
     * 一致性协议（standalone 为 LocalProtocol，ap 为 DistroProtocol）。
     */
    private final ConfigProtocol protocol;

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 订阅注册表。
     */
    private final SubscriptionRegistry registry;

    /**
     * 客户端长连接服务端。
     */
    private final NettyRemotingServer remotingServer;

    /**
     * HTTP 管理面。
     */
    private final HttpServer httpServer;

    /**
     * HTTP 处理线程池。
     */
    private final ExecutorService httpExecutor;

    /**
     * remoting 处理线程池。
     */
    private final ExecutorService remotingExecutor;

    /**
     * 变更推送器。
     */
    private final ChangePusher changePusher;

    /**
     * 是否已启动。
     */
    private volatile boolean started;

    /**
     * 是否已停止。
     */
    private volatile boolean stopped;

    /**
     * 创建服务端。
     *
     * @param options 启动配置
     * @throws IOException HTTP 端口绑定失败
     */
    public PolyConfigServer(ServerOptions options) throws IOException {
        if (options == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "options 不能为空");
        }
        this.options = options;
        this.protocol = createProtocol(options);
        this.configService = new ConfigService(protocol);
        this.registry = new SubscriptionRegistry(options.getMaxSubscriptionsPerConnection());
        this.remotingServer = new NettyRemotingServer(options.getHost(), options.getRemotingPort());
        this.httpServer = HttpServer.create(
                new InetSocketAddress(options.getHost(), options.getHttpPort()), 0);
        this.httpExecutor = Executors.newFixedThreadPool(16,
                daemonFactory("poly-config-http"));
        this.remotingExecutor = Executors.newCachedThreadPool(
                daemonFactory("poly-config-remoting"));

        this.httpServer.createContext("/v1/",
                new ApiHandler(configService, options.getMaxBodyBytes(), registry));
        this.httpServer.createContext("/", new StaticFileHandler());
        this.httpServer.setExecutor(httpExecutor);

        this.remotingServer.registerProcessor(ProtocolCodes.SUBSCRIBE,
                new SubscribeProcessor(configService, registry), remotingExecutor);
        this.remotingServer.registerProcessor(ProtocolCodes.UNSUBSCRIBE,
                new UnsubscribeProcessor(registry), remotingExecutor);
        this.remotingServer.registerProcessor(ProtocolCodes.BATCH_SUBSCRIBE,
                new BatchSubscribeProcessor(configService), remotingExecutor);
        this.remotingServer.registerProcessor(ProtocolCodes.FETCH,
                new FetchProcessor(configService), remotingExecutor);

        this.changePusher = new ChangePusher(remotingServer, registry);
        this.configService.addChangeListener(changePusher);
    }

    /**
     * 按运行模式创建一致性协议。
     *
     * @param options 启动配置
     * @return 一致性协议
     */
    private static ConfigProtocol createProtocol(ServerOptions options) {
        String mode = options.getMode().trim().toLowerCase(java.util.Locale.ROOT);
        switch (mode) {
            case "standalone":
                return new LocalProtocol(options.getDataDir(), options.getNodeId(),
                        options.getClusterId(), options.getHistoryLimit(),
                        options.getSnapshotThreshold());
            case "ap":
                return new DistroProtocol(options.getDataDir(), options.getNodeId(),
                        options.getClusterId(), options.getHistoryLimit(),
                        options.getSnapshotThreshold(), options.getClusterMembers());
            case "cp":
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "CP 模式由 Raft 实现，尚未提供（M1 里程碑）");
            default:
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "未知运行模式: " + options.getMode());
        }
    }

    /**
     * 启动服务端。
     */
    public synchronized void start() {
        if (stopped) {
            throw new PolyConfigException(ErrorCode.UNAVAILABLE, "服务端已停止");
        }
        if (started) {
            return;
        }
        remotingServer.start();
        httpServer.start();
        configService.start();
        String advertisedHost = resolveAdvertisedHost();
        protocol.setNodeAddress(advertisedHost, getHttpPort(), getRemotingPort());
        started = true;
        log.info("poly-config started, http={}:{}, remoting={}, advertised={}, dataDir={}",
                options.getHost(), getHttpPort(), getRemotingPort(), advertisedHost,
                options.getDataDir());
    }

    /**
     * 停止服务端，幂等。
     *
     * <p>按序优雅停机：先停 HTTP 接收并等待在途请求，再停 remoting 并等待
     * event loop 退出，然后关闭协议（最终快照 + 日志落盘），最后收线程池，
     * 每步都有超时兜底。</p>
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        long start = System.currentTimeMillis();
        stopped = true;
        configService.removeChangeListener(changePusher);
        // 1) 停止接收新 HTTP 请求，等待在途请求完成。
        // 注意：JDK HttpServer.stop(delay) 会实打实等待 delay 秒，取 2 秒作为折中
        httpServer.stop(2);
        // 2) 停止接收新长连接，等待 event loop 退出（最多 5 秒）
        remotingServer.shutdown();
        try {
            remotingServer.awaitTermination(5000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("等待 remoting 停机被中断");
        }
        // 3) 关闭协议：最终快照 + 日志刷盘 + 释放文件锁
        configService.close();
        // 4) 线程池收尾
        shutdownPool(httpExecutor, 3000L);
        shutdownPool(remotingExecutor, 3000L);
        started = false;
        log.info("poly-config stopped in {}ms", System.currentTimeMillis() - start);
    }

    private static void shutdownPool(ExecutorService pool, long timeoutMillis) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 返回实际 HTTP 端口。
     *
     * @return HTTP 端口
     */
    public int getHttpPort() {
        return httpServer.getAddress().getPort();
    }

    /**
     * 返回实际长连接端口。
     *
     * @return 长连接端口
     */
    public int getRemotingPort() {
        return remotingServer.localListenPort();
    }

    /**
     * 返回当前订阅条目总数（key × connection），用于观测。
     *
     * @return 订阅总数
     */
    public int getSubscriptionCount() {
        return registry.subscriptionCount();
    }

    private String resolveAdvertisedHost() {
        String configured = options.getAdvertiseHost();
        if (configured != null && !configured.trim().isEmpty()) {
            return configured.trim();
        }
        return NetworkUtils.resolveAdvertisedHost(options.getHost());
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
     * 启动入口。
     *
     * @param args 命令行参数
     * @throws Exception 启动失败
     */
    public static void main(String[] args) throws Exception {
        ServerOptions options = ServerOptions.fromArgs(args);
        PolyConfigServer server = new PolyConfigServer(options);
        server.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread(server::stop, "poly-config-shutdown"));
        log.info("poly-config 控制台: http://127.0.0.1:{}/", server.getHttpPort());
        Thread.currentThread().join();
    }
}
