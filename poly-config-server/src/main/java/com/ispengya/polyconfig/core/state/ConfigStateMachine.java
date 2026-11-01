package com.ispengya.polyconfig.core.state;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigChangeEvent;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigConstants;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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
        // 本地分配路径：version = revision（owner 本机与 standalone 语义不变）
        apply(command, revision, revision);
    }

    /**
     * 应用一条命令。
     *
     * <p>{@code version} 是跨节点权威序号：本地分配时等于 {@code revision}；
     * AP 副本路径传 owner 分配的版本（见 {@code ConfigCommand.assignedVersion}），
     * 此时 version 与节点本地 revision 解耦。</p>
     *
     * @param command 配置命令
     * @param revision 节点本地变更序号
     * @param version 条目版本（跨节点权威）
     */
    public synchronized void apply(ConfigCommand command, long revision, long version) {
        if (command == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "command 不能为空");
        }
        ConfigChangeEvent event;
        switch (command.getOperation()) {
            case PUBLISH:
            case ROLLBACK:
                event = applyPublish(command, revision, version);
                break;
            case DELETE:
                event = applyDelete(command, revision, version);
                break;
            default:
                throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                        "不支持的配置操作: " + command.getOperation());
        }
        notifyListeners(event);
    }

    private ConfigChangeEvent applyPublish(ConfigCommand command, long revision, long version) {
        ConfigKey key = command.getKey();
        String content = command.getContent();
        ConfigType type = command.getType();
        String md5 = Md5Utils.md5Hex(content);
        long now = command.getTimestamp();
        ConfigEntry existing = current.get(key);
        long createdTime = existing == null ? now : existing.getCreatedTime();
        ConfigEntry entry = new ConfigEntry(key, content, type, md5, version, revision,
                createdTime, now, command.getOperator());
        current.put(key, entry);
        addHistory(new HistoryEntry(key, revision, command.getOperation(), content, type, md5,
                command.getOperator(), now));
        return new ConfigChangeEvent(key, command.getOperation(), md5, version, revision, now);
    }

    private ConfigChangeEvent applyDelete(ConfigCommand command, long revision, long version) {
        ConfigKey key = command.getKey();
        ConfigEntry existing = current.remove(key);
        long now = command.getTimestamp();
        String content = existing == null ? "" : existing.getContent();
        ConfigType type = existing == null ? ConfigType.TEXT : existing.getType();
        String md5 = existing == null ? "" : existing.getMd5();
        addHistory(new HistoryEntry(key, revision, command.getOperation(), content, type, md5,
                command.getOperator(), now));
        return new ConfigChangeEvent(key, command.getOperation(), "", version, revision, now);
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
        String targetNamespace = normalize(namespace, ConfigConstants.DEFAULT_NAMESPACE);
        String targetGroup = normalize(group, ConfigConstants.DEFAULT_GROUP);
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
    public List<String> namespaces() {
        Set<String> result = new TreeSet<>();
        result.add(ConfigConstants.DEFAULT_NAMESPACE);
        for (ConfigKey key : current.keySet()) {
            result.add(key.getNamespace());
        }
        return new ArrayList<>(result);
    }

    @Override
    public List<String> groups(String namespace) {
        String targetNamespace = normalize(namespace, ConfigConstants.DEFAULT_NAMESPACE);
        Set<String> result = new TreeSet<>();
        result.add(ConfigConstants.DEFAULT_GROUP);
        for (ConfigKey key : current.keySet()) {
            if (key.getNamespace().equals(targetNamespace)) {
                result.add(key.getGroup());
            }
        }
        return new ArrayList<>(result);
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

    /**
     * 导出全部当前配置（供快照捕获，浅拷贝）。
     *
     * @return 当前配置列表
     */
    public List<ConfigEntry> exportEntries() {
        return new ArrayList<>(current.values());
    }

    /**
     * 导出全部历史（供快照捕获，每 key 拷贝，新的在前）。
     *
     * @return 历史表
     */
    public Map<ConfigKey, List<HistoryEntry>> exportHistories() {
        Map<ConfigKey, List<HistoryEntry>> result = new ConcurrentHashMap<>();
        for (Map.Entry<ConfigKey, Deque<HistoryEntry>> entry : histories.entrySet()) {
            synchronized (entry.getValue()) {
                result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
        }
        return result;
    }

    /**
     * 从快照恢复状态（仅供启动期、单线程场景调用）。
     *
     * @param entries 当前配置
     * @param historyLists 每 key 历史，新的在前
     */
    public void restore(List<ConfigEntry> entries, Map<ConfigKey, List<HistoryEntry>> historyLists) {
        current.clear();
        histories.clear();
        if (entries != null) {
            for (ConfigEntry entry : entries) {
                current.put(entry.getKey(), entry);
            }
        }
        if (historyLists != null) {
            for (Map.Entry<ConfigKey, List<HistoryEntry>> entry : historyLists.entrySet()) {
                histories.put(entry.getKey(), new ArrayDeque<>(entry.getValue()));
            }
        }
    }

    private static String normalize(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}
