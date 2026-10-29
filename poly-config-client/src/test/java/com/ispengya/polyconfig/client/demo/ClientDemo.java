package com.ispengya.polyconfig.client.demo;

import com.ispengya.polyconfig.client.ClientOptions;
import com.ispengya.polyconfig.client.PolyConfigClient;
import com.ispengya.polyconfig.common.error.PolyConfigException;

import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 常驻示例应用。
 *
 * <p>把 poly-config 当成正在使用的配置中心：启动时加载配置，运行期间持续输出
 * 生效参数，收到推送后立即更新；服务端不可用时从本地快照启动。</p>
 *
 * <p>建议在控制台维护 order-service.yml：</p>
 * <pre>
 * app:
 *   greeting: hello
 *   discount: false
 *   timeoutMillis: 3000
 * </pre>
 *
 * <p>修改 greeting、discount 或 timeoutMillis 后，本应用会打印配置变更并更新心跳输出。</p>
 */
public final class ClientDemo {

    private static final String DEFAULT_SERVER = "127.0.0.1:9848";
    private static final String DEFAULT_NAMESPACE = "public";
    private static final String DEFAULT_GROUP = "DEFAULT_GROUP";
    private static final String DEFAULT_DATA_ID = "order-service.yml";
    private static final String DEFAULT_SNAPSHOT_DIR = "target/client-demo-snapshot";

    private ClientDemo() {
    }

    /**
     * 启动示例应用。
     *
     * <p>支持参数：--server、--namespace、--group、--dataId、--snapshotDir、
     * --runSeconds。runSeconds 大于 0 时运行指定秒数后退出，默认常驻。</p>
     *
     * @param args 命令行参数
     * @throws Exception 启动失败
     */
    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        System.out.println("========================================");
        System.out.println(" poly-config 示例应用：order-service");
        System.out.println("========================================");
        System.out.println("  servers  = " + String.join(",", options.servers));
        System.out.println("  namespace= " + options.namespace);
        System.out.println("  group    = " + options.group);
        System.out.println("  dataId   = " + options.dataId);
        System.out.println("  snapshot = " + options.snapshotDir);
        System.out.println("----------------------------------------");
        System.out.println(" 建议在控制台维护以下配置：");
        System.out.println(" app:");
        System.out.println("   greeting: hello");
        System.out.println("   discount: false");
        System.out.println("   timeoutMillis: 3000");
        System.out.println("----------------------------------------");

        ClientOptions clientOptions = ClientOptions.builder()
                .servers(options.servers)
                .namespace(options.namespace)
                .snapshotDir(Paths.get(options.snapshotDir))
                .requestTimeoutMillis(3000L)
                .resyncIntervalMillis(10000L)
                .build();

        final PolyConfigClient client = new PolyConfigClient(clientOptions);
        final AtomicReference<AppSettings> settings =
                new AtomicReference<>(AppSettings.defaults());
        final CountDownLatch stop = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[app] 正在停止");
            client.close();
            stop.countDown();
        }, "client-demo-shutdown"));

        try {
            String initial = client.getConfig(options.group, options.dataId, 3000L);
            apply(initial, settings, "启动加载");
        } catch (PolyConfigException e) {
            System.out.println("[app] 服务端不可用，使用默认参数启动: " + e.getMessage());
        }

        client.addListener(options.group, options.dataId, (dataId, group, content) ->
                apply(content, settings, "配置变更"));

        ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "order-service-worker");
            thread.setDaemon(true);
            return thread;
        });
        worker.scheduleWithFixedDelay(() -> {
            AppSettings current = settings.get();
            System.out.println("[order-service] greeting=" + current.greeting
                    + ", discount=" + current.discount
                    + ", timeoutMillis=" + current.timeoutMillis);
        }, 5, 5, TimeUnit.SECONDS);

        System.out.println("[app] 已启动，修改控制台配置可观察实时推送");
        if (options.runSeconds > 0) {
            stop.await(options.runSeconds, TimeUnit.SECONDS);
        } else {
            System.out.println("[app] 按 Ctrl+C 退出");
            stop.await();
        }

        worker.shutdownNow();
        client.close();
    }

    private static void apply(String content, AtomicReference<AppSettings> settings, String reason) {
        if (content == null) {
            AppSettings defaults = AppSettings.defaults();
            settings.set(defaults);
            System.out.println("[" + reason + "] 配置不存在，使用默认参数: " + defaults);
            return;
        }
        System.out.println("[" + reason + "] 收到配置内容:");
        for (String line : content.split("\\r?\\n")) {
            System.out.println("  " + line);
        }
        AppSettings previous = settings.getAndSet(AppSettings.parse(content));
        System.out.println("[" + reason + "] 生效参数: " + settings.get());
        if (!Objects.equals(previous, settings.get())) {
            System.out.println("[" + reason + "] 参数已更新");
        }
    }

    /**
     * 应用运行参数。
     */
    private static final class AppSettings {

        /**
         * 问候语。
         */
        private final String greeting;

        /**
         * 是否开启折扣。
         */
        private final boolean discount;

        /**
         * 业务超时时间。
         */
        private final long timeoutMillis;

        private AppSettings(String greeting, boolean discount, long timeoutMillis) {
            this.greeting = greeting;
            this.discount = discount;
            this.timeoutMillis = timeoutMillis;
        }

        private static AppSettings defaults() {
            return new AppSettings("hello", false, 3000L);
        }

        private static AppSettings parse(String content) {
            String greeting = "hello";
            boolean discount = false;
            long timeoutMillis = 3000L;
            for (String line : content.split("\\r?\\n")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int separator = trimmed.indexOf(':');
                if (separator < 0) {
                    separator = trimmed.indexOf('=');
                }
                if (separator < 0) {
                    continue;
                }
                String key = trimmed.substring(0, separator).trim();
                String value = stripQuotes(trimmed.substring(separator + 1).trim());
                if ("greeting".equals(key)) {
                    greeting = value;
                } else if ("discount".equals(key)) {
                    discount = Boolean.parseBoolean(value);
                } else if ("timeoutMillis".equals(key) || "timeout".equals(key)) {
                    try {
                        timeoutMillis = Long.parseLong(value);
                    } catch (NumberFormatException ignored) {
                        // 非法值保持默认
                    }
                }
            }
            return new AppSettings(greeting, discount, timeoutMillis);
        }

        private static String stripQuotes(String value) {
            if (value.length() >= 2
                    && ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'")))) {
                return value.substring(1, value.length() - 1);
            }
            return value;
        }

        @Override
        public boolean equals(Object value) {
            if (this == value) {
                return true;
            }
            if (!(value instanceof AppSettings)) {
                return false;
            }
            AppSettings other = (AppSettings) value;
            return discount == other.discount
                    && timeoutMillis == other.timeoutMillis
                    && greeting.equals(other.greeting);
        }

        @Override
        public int hashCode() {
            return Objects.hash(greeting, discount, timeoutMillis);
        }

        @Override
        public String toString() {
            return "AppSettings{greeting='" + greeting + "', discount=" + discount
                    + ", timeoutMillis=" + timeoutMillis + '}';
        }
    }

    /**
     * 命令行参数。
     */
    private static final class Options {

        /**
         * 服务端地址列表。
         */
        private String[] servers = {DEFAULT_SERVER};

        /**
         * 命名空间。
         */
        private String namespace = DEFAULT_NAMESPACE;

        /**
         * 分组。
         */
        private String group = DEFAULT_GROUP;

        /**
         * 配置项 ID。
         */
        private String dataId = DEFAULT_DATA_ID;

        /**
         * 本地快照目录。
         */
        private String snapshotDir = DEFAULT_SNAPSHOT_DIR;

        /**
         * 运行秒数，0 表示常驻。
         */
        private long runSeconds;

        private static Options parse(String[] args) {
            Options options = new Options();
            if (args == null) {
                return options;
            }
            for (String arg : args) {
                if (arg == null || !arg.startsWith("--")) {
                    continue;
                }
                int separator = arg.indexOf('=');
                if (separator < 0) {
                    throw new IllegalArgumentException("参数格式应为 --key=value: " + arg);
                }
                String key = arg.substring(2, separator);
                String value = arg.substring(separator + 1);
                switch (key) {
                    case "server":
                        options.servers = value.split(",");
                        break;
                    case "namespace":
                        options.namespace = value;
                        break;
                    case "group":
                        options.group = value;
                        break;
                    case "dataId":
                        options.dataId = value;
                        break;
                    case "snapshotDir":
                        options.snapshotDir = value;
                        break;
                    case "runSeconds":
                        options.runSeconds = Long.parseLong(value);
                        break;
                    default:
                        throw new IllegalArgumentException("未知参数: " + key);
                }
            }
            return options;
        }
    }
}
