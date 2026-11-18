/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.shaded;

import org.junit.Test;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks the jar produced by {@code gremlin-shaded}. These checks run during {@code verify}, after shading and
 * Moditect have completed in the {@code package} phase.
 */
public class ShadedArtifactVerificationTest {

    private static final String JAR_PROPERTY = "gremlin.jpms.shadedJar";
    private static final String MODULE_NAME = "org.apache.tinkerpop.gremlin.shaded";
    private static final Pattern VERSIONED_MODULE_INFO = Pattern.compile("META-INF/versions/\\d+/module-info\\.class");
    private static final Pattern VERSIONED_CLASS = Pattern.compile("META-INF/versions/\\d+/(.*\\.class)$");

    private static Path jarPath() {
        final String path = System.getProperty(JAR_PROPERTY);
        assertNotNull("system property " + JAR_PROPERTY + " was not set - check the maven-failsafe-plugin " +
                "configuration in gremlin-shaded/pom.xml", path);
        final Path jar = Paths.get(path);
        assertTrue("jar does not exist at " + jar, Files.isRegularFile(jar));
        return jar;
    }

    @Test
    public void jarShouldHaveNoVersionedModuleDescriptor() throws Exception {
        try (JarFile jar = new JarFile(jarPath().toFile())) {
            assertNotNull("gremlin-shaded must carry a root module-info.class", jar.getEntry("module-info.class"));
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final String name = entries.nextElement().getName();
                assertFalse("gremlin-shaded must not carry a versioned module-info.class: " + name,
                        VERSIONED_MODULE_INFO.matcher(name).matches());
            }
        }
    }

    @Test
    public void versionedClassesShouldBeFullyRelocated() throws Exception {
        try (JarFile jar = new JarFile(jarPath().toFile())) {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                final Matcher matcher = VERSIONED_CLASS.matcher(entry.getName());
                if (matcher.matches()) {
                    assertTrue("unrelocated class at " + entry.getName(),
                            matcher.group(1).startsWith("org/apache/tinkerpop/shaded/"));
                }
            }
        }
    }

    @Test
    public void jarShouldHaveExpectedDescriptor() {
        final Path jar = jarPath();
        final Set<ModuleReference> refs = ModuleFinder.of(jar).findAll();
        assertEquals("expected exactly one module in " + jar, 1, refs.size());
        final ModuleDescriptor descriptor = refs.iterator().next().descriptor();
        assertFalse(jar + " must be an explicit module", descriptor.isAutomatic());
        assertEquals(MODULE_NAME, descriptor.name());

        final Set<String> exports = descriptor.exports().stream().map(ModuleDescriptor.Exports::source).
                collect(Collectors.toCollection(TreeSet::new));
        assertEquals("exports of " + MODULE_NAME, new TreeSet<>(descriptor.packages()), exports);
        assertTrue("all exports must be unqualified",
                descriptor.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified));
        assertTrue("every package must be relocated under org.apache.tinkerpop.shaded: " + descriptor.packages(),
                descriptor.packages().stream().allMatch(p -> p.startsWith("org.apache.tinkerpop.shaded.")));
        assertFalse(MODULE_NAME + " must not be an open module", descriptor.isOpen());
        assertTrue("opens of " + MODULE_NAME + ": " + descriptor.opens(), descriptor.opens().isEmpty());
        assertEquals(Set.of("org.apache.tinkerpop.shaded.jackson.databind.Module"), descriptor.uses());
        assertTrue("the jar excludes META-INF/services, so its descriptor must not provide services: " +
                descriptor.provides(), descriptor.provides().isEmpty());
        assertTrue(MODULE_NAME + " must require only JDK modules: " + descriptor.requires(),
                descriptor.requires().stream().allMatch(r -> ModuleFinder.ofSystem().find(r.name()).isPresent()));
    }
}
