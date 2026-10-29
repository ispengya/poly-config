package com.ispengya.polyconfig.consistency;

import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.HistoryEntry;

import java.util.List;
import java.util.Optional;

/**
 * 配置状态机。
 *
 * <p>协议实现只负责让命令按一致顺序提交，状态机负责确定性地应用命令并产生变更事件。</p>
 */
public interface StateMachine {

    /**
     * 应用一条已提交的命令。
     *
     * @param command 配置命令
     * @param revision 全局单调递增的变更序号
     */
    void apply(ConfigCommand command, long revision);

    /**
     * 读取配置当前版本。
     *
     * @param key 配置键
     * @return 配置条目
     */
    Optional<ConfigEntry> get(ConfigKey key);

    /**
     * 列出指定命名空间和分组下的配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @return 配置列表
     */
    List<ConfigEntry> list(String namespace, String group);

    /**
     * 查询配置历史，按版本从新到旧返回。
     *
     * @param key 配置键
     * @param limit 最大返回条数
     * @return 历史列表
     */
    List<HistoryEntry> history(ConfigKey key, int limit);

    /**
     * 返回当前存在配置的命名空间，至少包含默认命名空间。
     *
     * @return 命名空间列表
     */
    List<String> namespaces();

    /**
     * 返回指定命名空间下存在配置的分组，至少包含默认分组。
     *
     * @param namespace 命名空间
     * @return 分组列表
     */
    List<String> groups(String namespace);

    /**
     * 注册变更监听器。
     *
     * @param listener 监听器
     */
    void addChangeListener(ChangeListener listener);

    /**
     * 移除变更监听器。
     *
     * @param listener 监听器
     */
    void removeChangeListener(ChangeListener listener);
}
