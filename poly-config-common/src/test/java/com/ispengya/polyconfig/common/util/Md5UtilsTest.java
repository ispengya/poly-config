package com.ispengya.polyconfig.common.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * {@link Md5Utils} 单元测试。
 */
public class Md5UtilsTest {

    @Test
    public void shouldComputeMd5ForUtf8Content() {
        assertEquals("5d41402abc4b2a76b9719d911017c592", Md5Utils.md5Hex("hello"));
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", Md5Utils.md5Hex(null));
    }
}
