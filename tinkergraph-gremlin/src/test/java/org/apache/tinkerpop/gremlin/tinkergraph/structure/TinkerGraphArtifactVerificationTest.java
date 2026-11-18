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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.junit.Test;

import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks the regular and {@code min} jars produced by {@code tinkergraph-gremlin}. These checks run during
 * {@code verify}, after the {@code package} phase has built both artifacts.
 */
public class TinkerGraphArtifactVerificationTest {

    private static final String REGULAR_JAR_PROPERTY = "gremlin.jpms.tinkergraphJar";
    private static final String MIN_JAR_PROPERTY = "gremlin.jpms.tinkergraphMinJar";
    private static final String MODULE_NAME = "org.apache.tinkerpop.gremlin.tinkergraph";
    private static final String SERVICES_PREFIX = "META-INF/services/";

    private static Path jarPath(final String systemProperty) {
        final String path = System.getProperty(systemProperty);
        assertNotNull("system property " + systemProperty + " was not set - check the maven-failsafe-plugin " +
                "configuration in tinkergraph-gremlin/pom.xml", path);
        final Path jar = Paths.get(path);
        assertTrue("jar does not exist at " + jar, Files.isRegularFile(jar));
        return jar;
    }

    private static ModuleDescriptor explicitDescriptor(final Path jar) {
        final Set<ModuleReference> refs = ModuleFinder.of(jar).findAll();
        assertEquals("expected exactly one module in " + jar, 1, refs.size());
        final ModuleDescriptor descriptor = refs.iterator().next().descriptor();
        assertFalse(jar + " must be an explicit module", descriptor.isAutomatic());
        assertEquals(MODULE_NAME, descriptor.name());
        return descriptor;
    }

    @Test
    public void regularJarShouldHaveExpectedDescriptor() throws Exception {
        final Path jar = jarPath(REGULAR_JAR_PROPERTY);
        final ModuleDescriptor descriptor = explicitDescriptor(jar);
        final Set<String> hidden = new TreeSet<>(Arrays.asList(
                "org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map",
                "org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect"));
        assertTrue("hidden packages no longer exist: " + hidden, descriptor.packages().containsAll(hidden));
        final Set<String> expectedExports = new TreeSet<>(descriptor.packages());
        expectedExports.removeAll(hidden);
        final Set<String> exports = descriptor.exports().stream().map(ModuleDescriptor.Exports::source).
                collect(Collectors.toCollection(TreeSet::new));
        assertEquals("exports of " + MODULE_NAME, expectedExports, exports);
        assertTrue("all exports must be unqualified",
                descriptor.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified));
        assertFalse(MODULE_NAME + " must not be an open module", descriptor.isOpen());
        assertTrue("opens of " + MODULE_NAME + ": " + descriptor.opens(), descriptor.opens().isEmpty());
        assertTrue("uses of " + MODULE_NAME + ": " + descriptor.uses(), descriptor.uses().isEmpty());
        assertServicesMatchProvides(jar, descriptor);
    }

    @Test
    public void minJarShouldHaveTheSameDescriptorAsTheRegularJar() throws Exception {
        final Path regularPath = jarPath(REGULAR_JAR_PROPERTY);
        final Path minPath = jarPath(MIN_JAR_PROPERTY);
        final byte[] regular;
        final byte[] min;
        try (JarFile regularJar = new JarFile(regularPath.toFile());
             JarFile minJar = new JarFile(minPath.toFile())) {
            final JarEntry regularEntry = regularJar.getJarEntry("module-info.class");
            final JarEntry minEntry = minJar.getJarEntry("module-info.class");
            assertNotNull("the regular TinkerGraph jar must carry a module-info.class", regularEntry);
            assertNotNull("the min TinkerGraph jar must carry a module-info.class", minEntry);
            try (InputStream in = regularJar.getInputStream(regularEntry)) {
                regular = in.readAllBytes();
            }
            try (InputStream in = minJar.getInputStream(minEntry)) {
                min = in.readAllBytes();
            }
        }
        assertTrue("the min TinkerGraph jar must carry the same module-info.class as the regular jar",
                Arrays.equals(regular, min));
        assertEquals(explicitDescriptor(regularPath), explicitDescriptor(minPath));
    }

    private static void assertServicesMatchProvides(final Path jar, final ModuleDescriptor descriptor)
            throws Exception {
        final Map<String, Set<String>> registered = new TreeMap<>();
        try (JarFile jarFile = new JarFile(jar.toFile())) {
            final Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (entry.isDirectory() || !entry.getName().startsWith(SERVICES_PREFIX)) continue;
                final String service = entry.getName().substring(SERVICES_PREFIX.length());
                try (InputStream in = jarFile.getInputStream(entry)) {
                    registered.put(service, Arrays.stream(new String(in.readAllBytes(), StandardCharsets.UTF_8).
                            split("\\R")).map(l -> l.replaceAll("#.*", "").trim()).filter(l -> !l.isEmpty()).
                            collect(Collectors.toCollection(TreeSet::new)));
                }
            }
        }
        final Map<String, Set<String>> provided = new TreeMap<>();
        for (final ModuleDescriptor.Provides provides : descriptor.provides()) {
            provided.put(provides.service(), new TreeSet<>(provides.providers()));
        }
        assertEquals("META-INF/services of " + descriptor.name() + " must match its provides", registered, provided);
    }
}
