package com.ispengya.polyconfig.server;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 测试期日志配置回归：SLF4J 必须有真实绑定，控制台编码必须为 UTF-8。
 */
public class LoggingConfigTest {

    @Test
    public void shouldHaveRealSlf4jBindingInTests() {
        assertFalse("测试 classpath 缺少 SLF4J 绑定（NOPLoggerFactory），"
                        + "日志会静默丢失",
                LoggerFactory.getILoggerFactory() instanceof NOPLoggerFactory);
    }

    @Test
    public void shouldForceUtf8ConsoleEncoding() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/logback.xml")) {
            assertNotNull("logback.xml 应在 classpath 上", in);
            byte[] data = readAll(in);
            String xml = new String(data, StandardCharsets.UTF_8);
            assertTrue("logback 控制台编码应显式设置为 UTF-8",
                    xml.contains("<charset>UTF-8</charset>"));
        }
    }

    private static byte[] readAll(InputStream in) throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
