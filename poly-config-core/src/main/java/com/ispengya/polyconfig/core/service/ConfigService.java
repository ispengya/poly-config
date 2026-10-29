package com.ispengya.polyconfig.core.service;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.ConfigType;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.consistency.ChangeListener;
import com.ispengya.polyconfig.consistency.ClusterStatus;
import com.ispengya.polyconfig.consistency.ConfigProtocol;
import com.ispengya.polyconfig.consistency.ReadConsistency;

import java.util.List;
import java.util.Optional;

/**
 * 配置服务。
 *
 * <p>面向管理端提供发布、读取、删除、历史和回滚能力，不感知具体复制协议。</p>
 */
public class ConfigService {

    /**
     * 一致性协议。
     */
    private final ConfigProtocol protocol;

    /**
     * 创建配置服务。
     *
     * @param protocol 一致性协议
     */
    public ConfigService(ConfigProtocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 发布配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param content 配置内容
     * @param type 内容类型
     * @param operator 操作人
     * @return 提交结果
     */
    public MutationResult publish(String namespace, String group, String dataId,
                                  String content, String type, String operator) {
        ConfigKey key = new ConfigKey(namespace, group, dataId);
        ConfigCommand command = ConfigCommand.publish(key, content == null ? "" : content,
                ConfigType.fromSupported(type), normalizeOperator(operator));
        return protocol.propose(command);
    }

    /**
     * 读取配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @return 配置条目
     */
    public Optional<ConfigEntry> get(String namespace, String group, String dataId) {
        ConfigKey key = new ConfigKey(namespace, group, dataId);
        return protocol.read(key, ReadConsistency.LINEARIZABLE);
    }

    /**
     * 列出指定命名空间和分组下的配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @return 配置列表
     */
    public List<ConfigEntry> list(String namespace, String group) {
        return protocol.list(namespace, group);
    }

    /**
     * 删除配置。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param operator 操作人
     * @return 提交结果
     */
    public MutationResult delete(String namespace, String group, String dataId, String operator) {
        ConfigKey key = new ConfigKey(namespace, group, dataId);
        if (!protocol.read(key, ReadConsistency.LINEARIZABLE).isPresent()) {
            throw new PolyConfigException(ErrorCode.NOT_FOUND, "配置不存在: " + key.toPath());
        }
        return protocol.propose(ConfigCommand.delete(key, normalizeOperator(operator)));
    }

    /**
     * 查询配置历史。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param limit 最大返回条数
     * @return 历史列表
     */
    public List<HistoryEntry> history(String namespace, String group, String dataId, int limit) {
        ConfigKey key = new ConfigKey(namespace, group, dataId);
        return protocol.history(key, limit);
    }

    /**
     * 返回当前存在配置的命名空间。
     *
     * @return 命名空间列表
     */
    public List<String> namespaces() {
        return protocol.namespaces();
    }

    /**
     * 返回指定命名空间下存在配置的分组。
     *
     * @param namespace 命名空间
     * @return 分组列表
     */
    public List<String> groups(String namespace) {
        return protocol.groups(namespace);
    }

    /**
     * 创建命名空间。
     *
     * @param namespace 命名空间名称
     */
    public void createNamespace(String namespace) {
        protocol.createNamespace(namespace);
    }

    /**
     * 在命名空间下创建分组。
     *
     * @param namespace 命名空间
     * @param group 分组名称
     */
    public void createGroup(String namespace, String group) {
        protocol.createGroup(namespace, group);
    }

    /**
     * 回滚到指定历史版本。
     *
     * @param namespace 命名空间
     * @param group 分组
     * @param dataId 配置项 ID
     * @param revision 目标历史版本序号
     * @param operator 操作人
     * @return 提交结果
     */
    public MutationResult rollback(String namespace, String group, String dataId,
                                   long revision, String operator) {
        ConfigKey key = new ConfigKey(namespace, group, dataId);
        HistoryEntry target = null;
        for (HistoryEntry entry : protocol.history(key, Integer.MAX_VALUE)) {
            if (entry.getRevision() == revision) {
                target = entry;
                break;
            }
        }
        if (target == null) {
            throw new PolyConfigException(ErrorCode.NOT_FOUND, "历史版本不存在: " + revision);
        }
        String normalizedOperator = normalizeOperator(operator);
        if (target.getOperation() == ConfigOperation.DELETE) {
            return protocol.propose(ConfigCommand.delete(key, normalizedOperator));
        }
        return protocol.propose(ConfigCommand.rollback(key, target.getContent(),
                target.getType(), normalizedOperator));
    }

    /**
     * 返回当前节点视角的集群状态。
     *
     * @return 集群状态
     */
    public ClusterStatus clusterStatus() {
        return protocol.clusterStatus();
    }

    /**
     * 注册变更监听器。
     *
     * @param listener 监听器
     */
    public void addChangeListener(ChangeListener listener) {
        protocol.addChangeListener(listener);
    }

    /**
     * 移除变更监听器。
     *
     * @param listener 监听器
     */
    public void removeChangeListener(ChangeListener listener) {
        protocol.removeChangeListener(listener);
    }

    /**
     * 启动协议。
     */
    public void start() {
        protocol.start();
    }

    /**
     * 关闭协议。
     */
    public void close() {
        protocol.close();
    }

    private static String normalizeOperator(String operator) {
        return operator == null || operator.trim().isEmpty() ? "anonymous" : operator.trim();
    }
}
