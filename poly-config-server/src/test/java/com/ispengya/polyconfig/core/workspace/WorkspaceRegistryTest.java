package com.ispengya.polyconfig.core.workspace;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link WorkspaceRegistry} 单元测试。
 */
public class WorkspaceRegistryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldPersistNamespacesAndGroups() throws Exception {
        Path file = folder.newFolder("data").toPath().resolve("workspaces.json");

        WorkspaceRegistry registry = new WorkspaceRegistry(file);
        assertEquals(Arrays.asList("public"), registry.namespaces());
        assertEquals(Arrays.asList("DEFAULT_GROUP"), registry.groups("public"));

        registry.createNamespace("dev");
        registry.createGroup("dev", "ORDER");

        WorkspaceRegistry reloaded = new WorkspaceRegistry(file);
        assertEquals(Arrays.asList("dev", "public"), reloaded.namespaces());
        assertEquals(Arrays.asList("DEFAULT_GROUP", "ORDER"), reloaded.groups("dev"));
        assertEquals(Arrays.asList("DEFAULT_GROUP"), reloaded.groups("missing"));
        assertTrue(Files.isRegularFile(file));
    }

    @Test
    public void shouldPersistPrettyPrintedJson() throws Exception {
        Path file = folder.newFolder("pretty").toPath().resolve("workspaces.json");

        WorkspaceRegistry registry = new WorkspaceRegistry(file);
        registry.createNamespace("dev");

        String json = new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue("工作区元数据应为格式化 JSON（含换行）", json.contains("\n"));
        assertTrue("工作区元数据应包含缩进", json.matches("(?s).*\\n\\s+\"dev\".*"));

        WorkspaceRegistry reloaded = new WorkspaceRegistry(file);
        assertTrue(reloaded.namespaces().contains("dev"));
    }

    @Test
    public void shouldLoadLegacyCompactWorkspaceFile() throws Exception {
        Path file = folder.newFolder("legacy").toPath().resolve("workspaces.json");
        // 旧版紧凑单行格式仍可加载
        String compact = "{\"public\":[\"DEFAULT_GROUP\",\"ORDER\"]}";
        Files.write(file, compact.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        WorkspaceRegistry reloaded = new WorkspaceRegistry(file);
        assertTrue(reloaded.namespaces().contains("public"));
        assertTrue(reloaded.groups("public").contains("ORDER"));
    }
}
