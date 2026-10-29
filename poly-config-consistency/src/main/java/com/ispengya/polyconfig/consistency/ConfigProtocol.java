package com.ispengya.polyconfig.consistency;

import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;

import java.util.List;
import java.util.Optional;

/**
 * 配置一致性协议。
 *
 * <p>standalone、CP、AP 三种模式实现同一接口，上层服务不感知复制细节。</p>
 */
public interface ConfigProtocol extends AutoCloseable {

    /**
     * 启动协议。
     */
    void start();

    /**
     * 提交一条配置命令。
     *
     * @param command 配置命令
     * @return 提交结果
     */
    MutationResult propose(ConfigCommand command);

    /**
     * 读取配置。
     *
     * @param key 配置键
     * @param consistency 读一致性级别
     * @return 配置条目
     */
    Optional<ConfigEntry> read(ConfigKey key, ReadConsistency consistency);

    /**
     * 列出指定命名空间和分组下的配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @return 配置列表
     */
    List<ConfigEntry> list(String namespace, String group);

    /**
     * 查询配置历史。
     *
     * @param key 配置键
     * @param limit 最大返回条数
     * @return 历史列表
     */
    List<HistoryEntry> history(ConfigKey key, int limit);

    /**
     * 返回节点状态。
     *
     * @return 节点状态
     */
    NodeStatus status();

    /**
     * 返回当前节点视角的集群状态。
     *
     * <p>控制台通过任意节点的管理 API 获取该视图，不需要直接连接每个 server。</p>
     *
     * @return 集群状态
     */
    ClusterStatus clusterStatus();

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

    @Override
    void close();
}
