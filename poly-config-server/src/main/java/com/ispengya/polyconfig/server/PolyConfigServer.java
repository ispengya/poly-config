package com.ispengya.polyconfig.server;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.NetworkUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.polyconfig.server.http.ApiHandler;
import com.ispengya.polyconfig.server.http.StaticFileHandler;
import com.ispengya.polyconfig.server.push.ChangePusher;
import com.ispengya.polyconfig.server.push.SubscriptionRegistry;
import com.ispengya.polyconfig.server.remoting.FetchProcessor;
import com.ispengya.polyconfig.server.remoting.SubscribeProcessor;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * poly-config 服务端。
 *
 * <p>M0 只装配 standalone 协议；HTTP 管理面、客户端长连接和控制台静态资源
 * 都在同一个进程内。</p>
 */
public class PolyConfigServer {

    private static final Logger log = LoggerFactory.getLogger(PolyConfigServer.class);

    /**
     * 启动配置。
     */
    private final ServerOptions options;

    /**
     * 单机协议。
     */
    private final LocalProtocol protocol;

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
        if (!"standalone".equalsIgnoreCase(options.getMode())) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "M0 仅支持 standalone 模式，当前: " + options.getMode());
        }
        this.options = options;
        this.protocol = new LocalProtocol(options.getDataDir(), options.getNodeId(),
                options.getClusterId(), options.getHistoryLimit());
        this.configService = new ConfigService(protocol);
        this.registry = new SubscriptionRegistry();
        this.remotingServer = new NettyRemotingServer(options.getRemotingPort());
        this.httpServer = HttpServer.create(
                new InetSocketAddress(options.getHost(), options.getHttpPort()), 0);
        this.httpExecutor = Executors.newFixedThreadPool(16,
                daemonFactory("poly-config-http"));
        this.remotingExecutor = Executors.newCachedThreadPool(
                daemonFactory("poly-config-remoting"));

        this.httpServer.createContext("/v1/", new ApiHandler(configService));
        this.httpServer.createContext("/", new StaticFileHandler());
        this.httpServer.setExecutor(httpExecutor);

        this.remotingServer.registerProcessor(ProtocolCodes.SUBSCRIBE,
                new SubscribeProcessor(configService, registry), remotingExecutor);
        this.remotingServer.registerProcessor(ProtocolCodes.FETCH,
                new FetchProcessor(configService), remotingExecutor);

        this.changePusher = new ChangePusher(remotingServer, registry);
        this.configService.addChangeListener(changePusher);
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
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        configService.removeChangeListener(changePusher);
        httpServer.stop(0);
        remotingServer.shutdown();
        configService.close();
        httpExecutor.shutdownNow();
        remotingExecutor.shutdownNow();
        started = false;
        log.info("poly-config stopped");
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
