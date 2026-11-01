package com.ispengya.polyconfig.core.workspace;

import com.alibaba.fastjson2.TypeReference;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigConstants;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.common.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 命名空间与分组元数据。
 *
 * <p>M0 单机模式下持久化到数据目录的 workspaces.json；后续接入 CP/AP 时需要
 * 把这部分元数据也纳入复制日志。</p>
 */
public class WorkspaceRegistry {

    /**
     * 元数据文件。
     */
    private final Path file;

    /**
     * 命名空间到分组集合的映射。
     */
    private final ConcurrentHashMap<String, Set<String>> groupsByNamespace =
            new ConcurrentHashMap<>();

    /**
     * 创建或加载工作区元数据。
     *
     * @param file 元数据文件
     */
    public WorkspaceRegistry(Path file) {
        this.file = Objects.requireNonNull(file, "file");
        ensureDefault();
        load();
    }

    /**
     * 返回全部命名空间。
     *
     * @return 命名空间列表
     */
    public synchronized List<String> namespaces() {
        Set<String> result = new TreeSet<>(groupsByNamespace.keySet());
        result.add(ConfigConstants.DEFAULT_NAMESPACE);
        return new ArrayList<>(result);
    }

    /**
     * 返回指定命名空间下的分组。
     *
     * @param namespace 命名空间
     * @return 分组列表
     */
    public synchronized List<String> groups(String namespace) {
        String target = normalize(namespace);
        Set<String> result = new TreeSet<>();
        Set<String> groups = groupsByNamespace.get(target);
        if (groups != null) {
            result.addAll(groups);
        }
        result.add(ConfigConstants.DEFAULT_GROUP);
        return new ArrayList<>(result);
    }

    /**
     * 创建命名空间。
     *
     * @param namespace 命名空间名称
     */
    public synchronized void createNamespace(String namespace) {
        String target = Preconditions.checkNotBlank(namespace, "namespace");
        groupsByNamespace.computeIfAbsent(target, value -> ConcurrentHashMap.newKeySet())
                .add(ConfigConstants.DEFAULT_GROUP);
        persist();
    }

    /**
     * 在命名空间下创建分组。
     *
     * @param namespace 命名空间
     * @param group 分组名称
     */
    public synchronized void createGroup(String namespace, String group) {
        String targetNamespace = normalize(namespace);
        String targetGroup = Preconditions.checkNotBlank(group, "group");
        groupsByNamespace.computeIfAbsent(targetNamespace, value -> ConcurrentHashMap.newKeySet())
                .add(targetGroup);
        persist();
    }

    private void ensureDefault() {
        groupsByNamespace.computeIfAbsent(ConfigConstants.DEFAULT_NAMESPACE,
                value -> ConcurrentHashMap.newKeySet()).add(ConfigConstants.DEFAULT_GROUP);
    }

    private void load() {
        if (!Files.isRegularFile(file)) {
            return;
        }
        try {
            String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            if (json.trim().isEmpty()) {
                return;
            }
            Map<String, List<String>> data = JsonUtils.parseObject(json,
                    new TypeReference<Map<String, List<String>>>() {
                    });
            if (data == null) {
                return;
            }
            for (Map.Entry<String, List<String>> entry : data.entrySet()) {
                String namespace = entry.getKey();
                if (namespace == null || namespace.trim().isEmpty()) {
                    continue;
                }
                Set<String> groups = groupsByNamespace.computeIfAbsent(namespace.trim(),
                        value -> ConcurrentHashMap.newKeySet());
                if (entry.getValue() != null) {
                    for (String group : entry.getValue()) {
                        if (group != null && !group.trim().isEmpty()) {
                            groups.add(group.trim());
                        }
                    }
                }
            }
            ensureDefault();
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "读取工作区元数据失败: " + file, e);
        }
    }

    private void persist() {
        Map<String, List<String>> data = new TreeMap<>();
        for (Map.Entry<String, Set<String>> entry : groupsByNamespace.entrySet()) {
            data.put(entry.getKey(), new ArrayList<>(new TreeSet<>(entry.getValue())));
        }
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path temp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            Files.write(temp, JsonUtils.toPrettyJson(data).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR,
                    "写入工作区元数据失败: " + file, e);
        }
    }

    private static String normalize(String namespace) {
        return namespace == null || namespace.trim().isEmpty()
                ? ConfigConstants.DEFAULT_NAMESPACE : namespace.trim();
    }
}
