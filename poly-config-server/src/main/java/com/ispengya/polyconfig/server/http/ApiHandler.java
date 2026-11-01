package com.ispengya.polyconfig.server.http;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.protocol.ApiResponse;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.CreateGroupRequest;
import com.ispengya.polyconfig.common.protocol.CreateNamespaceRequest;
import com.ispengya.polyconfig.common.protocol.HistoryView;
import com.ispengya.polyconfig.common.protocol.PageView;
import com.ispengya.polyconfig.common.protocol.PublishRequest;
import com.ispengya.polyconfig.common.protocol.RollbackRequest;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 配置中心 HTTP API。
 */
public class ApiHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiHandler.class);

    /**
     * 默认请求体上限：1MB。
     */
    public static final long DEFAULT_MAX_BODY_BYTES = 1024 * 1024L;

    /**
     * 拒绝超限请求时有界排空的下限：排空预算至少为该值。
     */
    private static final long DRAIN_FLOOR_BYTES = 16L * 1024 * 1024;

    /**
     * 列表接口默认每页条数。
     */
    private static final int DEFAULT_PAGE_SIZE = 20;

    /**
     * 配置列表接口每页条数上限。
     */
    private static final int MAX_LIST_PAGE_SIZE = 200;

    /**
     * 历史接口每页条数上限。
     */
    private static final int MAX_HISTORY_PAGE_SIZE = 100;

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 请求体最大字节数，超限返回 413。
     */
    private final long maxBodyBytes;

    /**
     * 创建 HTTP API 处理器（请求体上限取默认 1MB）。
     *
     * @param configService 配置服务
     */
    public ApiHandler(ConfigService configService) {
        this(configService, DEFAULT_MAX_BODY_BYTES);
    }

    /**
     * 创建 HTTP API 处理器。
     *
     * @param configService 配置服务
     * @param maxBodyBytes 请求体最大字节数
     */
    public ApiHandler(ConfigService configService, long maxBodyBytes) {
        this.configService = configService;
        this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();

            if ("GET".equalsIgnoreCase(method) && "/v1/cs/health".equals(path)) {
                writeJson(exchange, 200, ApiResponse.ok("UP"));
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/cluster".equals(path)) {
                writeJson(exchange, 200, ApiResponse.ok(configService.clusterStatus()));
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/namespaces".equals(path)) {
                writeJson(exchange, 200, ApiResponse.ok(configService.namespaces()));
                return;
            }
            if ("POST".equalsIgnoreCase(method) && "/v1/cs/namespaces".equals(path)) {
                handleCreateNamespace(exchange);
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/groups".equals(path)) {
                writeJson(exchange, 200,
                        ApiResponse.ok(configService.groups(query(exchange).get("namespace"))));
                return;
            }
            if ("POST".equalsIgnoreCase(method) && "/v1/cs/groups".equals(path)) {
                handleCreateGroup(exchange);
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/configs/list".equals(path)) {
                handleListConfigs(exchange);
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/configs".equals(path)) {
                handleGetConfig(exchange);
                return;
            }
            if ("POST".equalsIgnoreCase(method) && "/v1/cs/configs".equals(path)) {
                handlePublishConfig(exchange);
                return;
            }
            if ("DELETE".equalsIgnoreCase(method) && "/v1/cs/configs".equals(path)) {
                handleDeleteConfig(exchange);
                return;
            }
            if ("GET".equalsIgnoreCase(method) && "/v1/cs/history".equals(path)) {
                handleHistory(exchange);
                return;
            }
            if ("POST".equalsIgnoreCase(method) && "/v1/cs/history/rollback".equals(path)) {
                handleRollback(exchange);
                return;
            }
            writeJson(exchange, 404,
                    ApiResponse.error(ErrorCode.NOT_FOUND.getCode(), "接口不存在"));
        } catch (PolyConfigException e) {
            log.warn("配置 API 业务异常: {}", e.getMessage());
            writeJson(exchange, httpStatus(e.getErrorCode()),
                    ApiResponse.error(e.getErrorCode().getCode(), e.getMessage()));
        } catch (RuntimeException e) {
            log.error("配置 API 处理失败", e);
            writeJson(exchange, 500,
                    ApiResponse.error(ErrorCode.INTERNAL.getCode(), "内部错误"));
        } finally {
            exchange.close();
        }
    }

    private void handleCreateNamespace(HttpExchange exchange) throws IOException {
        CreateNamespaceRequest request = JsonUtils.parseObject(readBody(exchange),
                CreateNamespaceRequest.class);
        if (request == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "创建命名空间请求体为空");
        }
        configService.createNamespace(request.getNamespace());
        writeJson(exchange, 200, ApiResponse.ok(request.getNamespace()));
    }

    private void handleCreateGroup(HttpExchange exchange) throws IOException {
        CreateGroupRequest request = JsonUtils.parseObject(readBody(exchange),
                CreateGroupRequest.class);
        if (request == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "创建分组请求体为空");
        }
        configService.createGroup(request.getNamespace(), request.getGroup());
        writeJson(exchange, 200, ApiResponse.ok(request.getGroup()));
    }

    private void handleGetConfig(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        Optional<ConfigEntry> entry = configService.get(query.get("namespace"),
                query.get("group"), query.get("dataId"));
        if (!entry.isPresent()) {
            writeJson(exchange, 404,
                    ApiResponse.error(ErrorCode.NOT_FOUND.getCode(), "配置不存在"));
            return;
        }
        writeJson(exchange, 200, ApiResponse.ok(ConfigView.from(entry.get())));
    }

    private void handleListConfigs(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        int page = pageParam(query);
        int size = sizeParam(query, DEFAULT_PAGE_SIZE, MAX_LIST_PAGE_SIZE);
        String keyword = query.get("keyword");

        List<ConfigEntry> entries = configService.list(query.get("namespace"), query.get("group"));
        if (keyword != null && !keyword.trim().isEmpty()) {
            String needle = keyword.trim().toLowerCase(Locale.ROOT);
            List<ConfigEntry> matched = new ArrayList<>();
            for (ConfigEntry entry : entries) {
                if (entry.getKey().getDataId().toLowerCase(Locale.ROOT).contains(needle)) {
                    matched.add(entry);
                }
            }
            entries = matched;
        }
        List<ConfigEntry> sorted = new ArrayList<>(entries);
        Collections.sort(sorted, new Comparator<ConfigEntry>() {
            @Override
            public int compare(ConfigEntry left, ConfigEntry right) {
                int byTime = Long.compare(right.getModifiedTime(), left.getModifiedTime());
                if (byTime != 0) {
                    return byTime;
                }
                return left.getKey().getDataId().compareTo(right.getKey().getDataId());
            }
        });
        List<ConfigEntry> slice = pageSlice(sorted, page, size);
        List<ConfigView> views = new ArrayList<>(slice.size());
        for (ConfigEntry entry : slice) {
            views.add(ConfigView.from(entry));
        }
        writeJson(exchange, 200,
                ApiResponse.ok(new PageView<ConfigView>(views, sorted.size(), page, size)));
    }

    private void handlePublishConfig(HttpExchange exchange) throws IOException {
        PublishRequest request = JsonUtils.parseObject(readBody(exchange), PublishRequest.class);
        if (request == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "发布请求体为空");
        }
        writeJson(exchange, 200, ApiResponse.ok(configService.publish(
                request.getNamespace(), request.getGroup(), request.getDataId(),
                request.getContent(), request.getType(), request.getOperator(),
                request.getExpectedVersion())));
    }

    private void handleDeleteConfig(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        writeJson(exchange, 200, ApiResponse.ok(configService.delete(
                query.get("namespace"), query.get("group"), query.get("dataId"),
                query.get("operator"))));
    }

    private void handleHistory(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        int page = pageParam(query);
        int size = sizeParam(query, DEFAULT_PAGE_SIZE, MAX_HISTORY_PAGE_SIZE);
        // 历史按 key 保留、总量有界（historyLimit），取全量后再分页以得到精确 total
        List<HistoryEntry> all = configService.history(query.get("namespace"),
                query.get("group"), query.get("dataId"), Integer.MAX_VALUE);
        List<HistoryEntry> slice = pageSlice(all, page, size);
        List<HistoryView> views = new ArrayList<>(slice.size());
        for (HistoryEntry entry : slice) {
            views.add(HistoryView.from(entry));
        }
        writeJson(exchange, 200,
                ApiResponse.ok(new PageView<HistoryView>(views, all.size(), page, size)));
    }

    private void handleRollback(HttpExchange exchange) throws IOException {
        RollbackRequest request = JsonUtils.parseObject(readBody(exchange), RollbackRequest.class);
        if (request == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "回滚请求体为空");
        }
        writeJson(exchange, 200, ApiResponse.ok(configService.rollback(
                request.getNamespace(), request.getGroup(), request.getDataId(),
                request.getRevision(), request.getOperator())));
    }

    /**
     * 读取请求体，超过 {@link #maxBodyBytes} 拒绝。
     *
     * <p>拒绝前先有界排空客户端已发送的数据：带着未读数据关连接会触发
     * TCP RST，把已发出的 413 响应一起冲掉，客户端只能看到连接重置。
     * 排空有预算上限（见 {@link #drainLimitBytes()}），超大声明不会被读完，
     * 服务端内存与读取量仍有界。</p>
     *
     * @param exchange HTTP 交换
     * @return 请求体字符串
     * @throws IOException 读取失败
     */
    private String readBody(HttpExchange exchange) throws IOException {
        String declaredLength = exchange.getRequestHeaders().getFirst("Content-Length");
        if (declaredLength != null) {
            long declared;
            try {
                declared = Long.parseLong(declaredLength);
            } catch (NumberFormatException e) {
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "Content-Length 格式错误: " + declaredLength);
            }
            if (declared > maxBodyBytes) {
                drainBounded(exchange, Math.min(declared, drainLimitBytes()));
                throw new PolyConfigException(ErrorCode.PAYLOAD_TOO_LARGE,
                        "请求体超过上限: " + maxBodyBytes + " 字节");
            }
        }
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        long total = 0;
        try (InputStream input = exchange.getRequestBody()) {
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > maxBodyBytes) {
                    drainStream(input, drainLimitBytes());
                    throw new PolyConfigException(ErrorCode.PAYLOAD_TOO_LARGE,
                            "请求体超过上限: " + maxBodyBytes + " 字节");
                }
                result.write(buffer, 0, read);
            }
        }
        return new String(result.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * 有界排空预算：至少 {@link #DRAIN_FLOOR_BYTES}，或请求体上限的两倍。
     *
     * @return 排空可读取的最大字节数
     */
    private long drainLimitBytes() {
        return Math.max(DRAIN_FLOOR_BYTES, maxBodyBytes * 2);
    }

    /**
     * 从请求流中有界排空，读满预算或到达流末尾即停止。
     *
     * @param input 请求流
     * @param limit 最多读取字节数
     * @throws IOException 读取失败
     */
    private static void drainStream(InputStream input, long limit) throws IOException {
        byte[] buffer = new byte[8192];
        long drained = 0;
        while (drained < limit) {
            int read = input.read(buffer, 0, (int) Math.min(buffer.length, limit - drained));
            if (read < 0) {
                return;
            }
            drained += read;
        }
    }

    /**
     * 打开请求流并有界排空；排空失败不影响超限拒绝。
     *
     * @param exchange HTTP 交换
     * @param limit 最多读取字节数
     */
    private static void drainBounded(HttpExchange exchange, long limit) {
        try (InputStream input = exchange.getRequestBody()) {
            drainStream(input, limit);
        } catch (IOException e) {
            log.debug("超限请求排空中断（忽略，按超限拒绝）: {}", e.toString());
        }
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> result = new HashMap<>();
        String rawQuery = exchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            int separator = pair.indexOf('=');
            String key = separator < 0 ? pair : pair.substring(0, separator);
            String value = separator < 0 ? "" : pair.substring(separator + 1);
            try {
                result.put(URLDecoder.decode(key, StandardCharsets.UTF_8.name()),
                        URLDecoder.decode(value, StandardCharsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 not supported", e);
            }
        }
        return result;
    }

    private static int intValue(String name, String value, int defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "参数 " + name + " 应为整数: " + value);
        }
    }

    /**
     * 解析并校验 page 参数（从 1 开始）。
     *
     * @param query 查询参数
     * @return 页码
     */
    private static int pageParam(Map<String, String> query) {
        int page = intValue("page", query.get("page"), 1);
        if (page < 1) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "page 必须 ≥ 1: " + page);
        }
        return page;
    }

    /**
     * 解析并校验 size 参数。
     *
     * @param query 查询参数
     * @param defaultValue 缺省值
     * @param maxValue 上限
     * @return 每页条数
     */
    private static int sizeParam(Map<String, String> query, int defaultValue, int maxValue) {
        int size = intValue("size", query.get("size"), defaultValue);
        if (size < 1 || size > maxValue) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "size 必须在 1.." + maxValue + " 之间: " + size);
        }
        return size;
    }

    /**
     * 按页码切片，页码越界返回空列表；用 long 计算起点避免乘法溢出。
     *
     * @param all 全量条目
     * @param page 页码（从 1 开始）
     * @param size 每页条数
     * @param <T> 条目类型
     * @return 当前页条目
     */
    private static <T> List<T> pageSlice(List<T> all, int page, int size) {
        long from = (long) (page - 1) * size;
        if (from >= all.size()) {
            return Collections.emptyList();
        }
        int to = (int) Math.min(from + size, all.size());
        return new ArrayList<>(all.subList((int) from, to));
    }

    private static void writeJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = JsonUtils.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private static int httpStatus(ErrorCode errorCode) {
        switch (errorCode) {
            case INVALID_ARGUMENT:
                return 400;
            case PAYLOAD_TOO_LARGE:
                return 413;
            case NOT_FOUND:
                return 404;
            case VERSION_CONFLICT:
                return 409;
            case NOT_LEADER:
            case UNAVAILABLE:
                return 503;
            default:
                return 500;
        }
    }
}
