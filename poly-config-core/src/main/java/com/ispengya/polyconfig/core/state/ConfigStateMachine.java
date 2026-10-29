package com.ispengya.polyconfig.core.state;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigChangeEvent;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.util.Md5Utils;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 配置状态机。
 *
 * <p>只依赖已提交命令和全局 revision，应用结果必须与节点、时钟无关。
 * apply 由协议层串行调用，读路径允许并发。</p>
 */
public class ConfigStateMachine implements StateMachine {

    private static final Logger log = LoggerFactory.getLogger(ConfigStateMachine.class);

    /**
     * 当前配置，按 ConfigKey 排序。
     */
    private final ConcurrentSkipListMap<ConfigKey, ConfigEntry> current =
            new ConcurrentSkipListMap<>();

    /**
     * 每个配置键的历史版本，新版本在前。
     */
    private final ConcurrentHashMap<ConfigKey, Deque<HistoryEntry>> histories =
            new ConcurrentHashMap<>();

    /**
     * 变更监听器。
     */
    private final CopyOnWriteArrayList<ChangeListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * 每个配置键保留的历史条数。
     */
    private final int historyLimit;

    /**
     * 创建配置状态机。
     *
     * @param historyLimit 每个配置键保留的历史条数
     */
    public ConfigStateMachine(int historyLimit) {
        if (historyLimit <= 0) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "historyLimit 必须大于 0");
        }
        this.historyLimit = historyLimit;
    }

    @Override
    public synchronized void apply(ConfigCommand command, long revision) {
        if (command == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "command 不能为空");
        }
        ConfigChangeEvent event;
        switch (command.getOperation()) {
            case PUBLISH:
            case ROLLBACK:
                event = applyPublish(command, revision);
                break;
            case DELETE:
                event = applyDelete(command, revision);
                break;
            default:
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "不支持的配置操作: " + command.getOperation());
        }
        notifyListeners(event);
    }

    private ConfigChangeEvent applyPublish(ConfigCommand command, long revision) {
        ConfigKey key = command.getKey();
        String content = command.getContent();
        ConfigType type = command.getType();
        String md5 = Md5Utils.md5Hex(content);
        long now = command.getTimestamp();
        ConfigEntry existing = current.get(key);
        long createdTime = existing == null ? now : existing.getCreatedTime();
        ConfigEntry entry = new ConfigEntry(key, content, type, md5, revision, revision,
                createdTime, now, command.getOperator());
        current.put(key, entry);
        addHistory(new HistoryEntry(key, revision, command.getOperation(), content, type, md5,
                command.getOperator(), now));
        return new ConfigChangeEvent(key, command.getOperation(), md5, revision, revision, now);
    }

    private ConfigChangeEvent applyDelete(ConfigCommand command, long revision) {
        ConfigKey key = command.getKey();
        ConfigEntry existing = current.remove(key);
        long now = command.getTimestamp();
        String content = existing == null ? "" : existing.getContent();
        ConfigType type = existing == null ? ConfigType.TEXT : existing.getType();
        String md5 = existing == null ? "" : existing.getMd5();
        addHistory(new HistoryEntry(key, revision, command.getOperation(), content, type, md5,
                command.getOperator(), now));
        return new ConfigChangeEvent(key, command.getOperation(), "", revision, revision, now);
    }

    private void addHistory(HistoryEntry entry) {
        Deque<HistoryEntry> deque = histories.computeIfAbsent(entry.getKey(),
                key -> new ArrayDeque<HistoryEntry>());
        synchronized (deque) {
            deque.addFirst(entry);
            while (deque.size() > historyLimit) {
                deque.removeLast();
            }
        }
    }

    private void notifyListeners(ConfigChangeEvent event) {
        for (ChangeListener listener : listeners) {
            try {
                listener.onChange(event);
            } catch (RuntimeException e) {
                log.warn("配置变更监听器执行失败, key={}", event.getKey(), e);
            }
        }
    }

    @Override
    public Optional<ConfigEntry> get(ConfigKey key) {
        return Optional.ofNullable(current.get(key));
    }

    @Override
    public List<ConfigEntry> list(String namespace, String group) {
        String targetNamespace = normalize(namespace,
                com.ispengya.polyconfig.common.model.ConfigConstants.DEFAULT_NAMESPACE);
        String targetGroup = normalize(group,
                com.ispengya.polyconfig.common.model.ConfigConstants.DEFAULT_GROUP);
        List<ConfigEntry> result = new ArrayList<>();
        for (ConfigEntry entry : current.values()) {
            if (entry.getKey().getNamespace().equals(targetNamespace)
                    && entry.getKey().getGroup().equals(targetGroup)) {
                result.add(entry);
            }
        }
        return result;
    }

    @Override
    public List<HistoryEntry> history(ConfigKey key, int limit) {
        Deque<HistoryEntry> deque = histories.get(key);
        if (deque == null) {
            return Collections.emptyList();
        }
        int target = limit <= 0 ? historyLimit : Math.min(limit, historyLimit);
        List<HistoryEntry> result = new ArrayList<>(target);
        synchronized (deque) {
            for (HistoryEntry entry : deque) {
                if (result.size() >= target) {
                    break;
                }
                result.add(entry);
            }
        }
        return result;
    }

    @Override
    public void addChangeListener(ChangeListener listener) {
        if (listener != null) {
            listeners.addIfAbsent(listener);
        }
    }

    @Override
    public void removeChangeListener(ChangeListener listener) {
        listeners.remove(listener);
    }

    private static String normalize(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}
