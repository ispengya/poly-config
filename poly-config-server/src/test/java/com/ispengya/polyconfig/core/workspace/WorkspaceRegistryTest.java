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
}
