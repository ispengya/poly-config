package com.ispengya.polyconfig.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 工具。
 *
 * <p>用于配置内容指纹比对，不作为安全哈希使用。</p>
 */
public final class Md5Utils {

    /**
     * 十六进制字符表。
     */
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Md5Utils() {
    }

    /**
     * 计算字符串内容的 MD5 十六进制值。
     *
     * @param content 配置内容，null 按空字符串处理
     * @return 32 位小写十六进制 MD5
     */
    public static String md5Hex(String content) {
        String value = content == null ? "" : content;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            char[] result = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                int current = bytes[i] & 0xFF;
                result[i * 2] = HEX[current >>> 4];
                result[i * 2 + 1] = HEX[current & 0x0F];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    }
}
