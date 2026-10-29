package com.ispengya.polyconfig.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 控制台静态资源处理器。
 *
 * <p>只暴露固定的几个资源，不拼接用户输入路径。</p>
 */
public class StaticFileHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendEmpty(exchange, 405);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String resource;
            String contentType;
            switch (path) {
                case "/":
                case "/index.html":
                    resource = "/web/index.html";
                    contentType = "text/html; charset=UTF-8";
                    break;
                case "/app.js":
                    resource = "/web/app.js";
                    contentType = "application/javascript; charset=UTF-8";
                    break;
                case "/style.css":
                    resource = "/web/style.css";
                    contentType = "text/css; charset=UTF-8";
                    break;
                default:
                    sendEmpty(exchange, 404);
                    return;
            }

            InputStream input = StaticFileHandler.class.getResourceAsStream(resource);
            if (input == null) {
                sendEmpty(exchange, 404);
                return;
            }
            byte[] bytes;
            try {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    output.write(buffer, 0, read);
                }
                bytes = output.toByteArray();
            } finally {
                input.close();
            }

            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        } finally {
            exchange.close();
        }
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }
}
