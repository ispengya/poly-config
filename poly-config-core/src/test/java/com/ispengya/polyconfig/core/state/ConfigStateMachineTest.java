package com.ispengya.polyconfig.core.state;

import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link ConfigStateMachine} 单元测试。
 */
public class ConfigStateMachineTest {

    @Test
    public void shouldApplyPublishAndKeepHistoryLimit() {
        ConfigStateMachine stateMachine = new ConfigStateMachine(2);
        AtomicInteger events = new AtomicInteger();
        stateMachine.addChangeListener(event -> events.incrementAndGet());
        ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "app.yml");

        stateMachine.apply(ConfigCommand.publish(key, "v1", ConfigType.YAML, "alice"), 1L);
        stateMachine.apply(ConfigCommand.publish(key, "v2", ConfigType.YAML, "alice"), 2L);
        stateMachine.apply(ConfigCommand.publish(key, "v3", ConfigType.YAML, "alice"), 3L);

        Optional<ConfigEntry> entry = stateMachine.get(key);
        assertTrue(entry.isPresent());
        assertEquals("v3", entry.get().getContent());
        assertEquals(3L, entry.get().getVersion());

        List<HistoryEntry> history = stateMachine.history(key, 10);
        assertEquals(2, history.size());
        assertEquals(3L, history.get(0).getRevision());
        assertEquals(2L, history.get(1).getRevision());
        assertEquals(3, events.get());

        assertEquals(1, stateMachine.list("public", "DEFAULT_GROUP").size());
    }

    @Test
    public void shouldDeleteAndRecordHistory() {
        ConfigStateMachine stateMachine = new ConfigStateMachine(10);
        ConfigKey key = new ConfigKey("public", "DEFAULT_GROUP", "app.yml");

        stateMachine.apply(ConfigCommand.publish(key, "v1", ConfigType.TEXT, "alice"), 1L);
        stateMachine.apply(ConfigCommand.delete(key, "bob"), 2L);

        assertFalse(stateMachine.get(key).isPresent());
        List<HistoryEntry> history = stateMachine.history(key, 10);
        assertEquals(2, history.size());
        assertEquals("DELETE", history.get(0).getOperation().name());
        assertEquals("v1", history.get(0).getContent());
        assertEquals("bob", history.get(0).getOperator());
    }
}
