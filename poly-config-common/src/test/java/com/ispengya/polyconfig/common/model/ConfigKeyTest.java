package com.ispengya.polyconfig.common.model;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * {@link ConfigKey} 单元测试。
 */
public class ConfigKeyTest {

    @Test
    public void shouldUseDefaultNamespaceAndGroup() {
        ConfigKey key = new ConfigKey(null, " ", "app.yml");

        assertEquals(ConfigConstants.DEFAULT_NAMESPACE, key.getNamespace());
        assertEquals(ConfigConstants.DEFAULT_GROUP, key.getGroup());
        assertEquals("app.yml", key.getDataId());
        assertEquals("public/DEFAULT_GROUP/app.yml", key.toPath());
    }

    @Test
    public void shouldRejectBlankDataId() {
        try {
            new ConfigKey("public", "DEFAULT_GROUP", " ");
            fail("expected PolyConfigException");
        } catch (PolyConfigException e) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, e.getErrorCode());
        }
    }

    @Test
    public void shouldCompareByValue() {
        ConfigKey first = new ConfigKey("prod", "order", "app.yml");
        ConfigKey second = new ConfigKey("prod", "order", "app.yml");
        ConfigKey other = new ConfigKey("dev", "order", "app.yml");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, other);
        assertEquals(0, first.compareTo(second));
    }
}
