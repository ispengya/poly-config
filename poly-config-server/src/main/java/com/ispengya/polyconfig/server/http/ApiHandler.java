package com.ispengya.polyconfig.server.http;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.protocol.ApiResponse;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.HistoryView;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 配置中心 HTTP API。
 */
public class ApiHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiHandler.class);

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 创建 HTTP API 处理器。
     *
     * @param configService 配置服务
     */
    public ApiHandler(ConfigService configService) {
        this.configService = configService;
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
        List<ConfigEntry> entries = configService.list(query.get("namespace"), query.get("group"));
        List<ConfigView> views = new ArrayList<>(entries.size());
        for (ConfigEntry entry : entries) {
            views.add(ConfigView.from(entry));
        }
        writeJson(exchange, 200, ApiResponse.ok(views));
    }

    private void handlePublishConfig(HttpExchange exchange) throws IOException {
        PublishRequest request = JsonUtils.parseObject(readBody(exchange), PublishRequest.class);
        if (request == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "发布请求体为空");
        }
        writeJson(exchange, 200, ApiResponse.ok(configService.publish(
                request.getNamespace(), request.getGroup(), request.getDataId(),
                request.getContent(), request.getType(), request.getOperator())));
    }

    private void handleDeleteConfig(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        writeJson(exchange, 200, ApiResponse.ok(configService.delete(
                query.get("namespace"), query.get("group"), query.get("dataId"),
                query.get("operator"))));
    }

    private void handleHistory(HttpExchange exchange) throws IOException {
        Map<String, String> query = query(exchange);
        int limit = intValue(query.get("limit"), 20);
        List<HistoryEntry> entries = configService.history(query.get("namespace"),
                query.get("group"), query.get("dataId"), limit);
        List<HistoryView> views = new ArrayList<>(entries.size());
        for (HistoryEntry entry : entries) {
            views.add(HistoryView.from(entry));
        }
        writeJson(exchange, 200, ApiResponse.ok(views));
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

    private static String readBody(HttpExchange exchange) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        try (InputStream input = exchange.getRequestBody()) {
            while ((read = input.read(buffer)) != -1) {
                result.write(buffer, 0, read);
            }
        }
        return new String(result.toByteArray(), StandardCharsets.UTF_8);
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

    private static int intValue(String value, int defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "整数参数格式错误: " + value);
        }
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
