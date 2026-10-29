package com.ispengya.polyconfig.common.model;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link ConfigType} 单元测试。
 */
public class ConfigTypeTest {

    @Test
    public void shouldSupportOnlyYamlJsonAndProperties() {
        assertEquals(ConfigType.YAML, ConfigType.fromSupported("yaml"));
        assertEquals(ConfigType.JSON, ConfigType.fromSupported("JSON"));
        assertEquals(ConfigType.PROPERTIES, ConfigType.fromSupported("Properties"));
        assertEquals(ConfigType.YAML, ConfigType.fromSupported(null));
    }

    @Test
    public void shouldRejectUnsupportedFormat() {
        try {
            ConfigType.fromSupported("XML");
            fail("expected PolyConfigException");
        } catch (PolyConfigException e) {
            assertEquals(ErrorCode.INVALID_ARGUMENT, e.getErrorCode());
        }
    }
}
