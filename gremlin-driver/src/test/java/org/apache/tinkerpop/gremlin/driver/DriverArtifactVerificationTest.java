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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.net.URL;
import java.net.URLClassLoader;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks the regular and shaded jars produced by {@code gremlin-driver}. These checks run during {@code verify},
 * after the {@code package} phase has built both artifacts.
 */
public class DriverArtifactVerificationTest {

    private static final String DRIVER_JAR_PROPERTY = "gremlin.jpms.driverJar";
    private static final String DRIVER_SHADED_JAR_PROPERTY = "gremlin.jpms.driverShadedJar";
    private static final String DRIVER_MODULE = "org.apache.tinkerpop.gremlin.driver";
    private static final String TINKERGRAPH_MODULE = "org.apache.tinkerpop.gremlin.tinkergraph";
    private static final String SERVICES_PREFIX = "META-INF/services/";
    private static final Pattern VERSIONED_MODULE_INFO = Pattern.compile("META-INF/versions/\\d+/module-info\\.class");
    private static final Pattern SIGNATURE_FILE = Pattern.compile("META-INF/[^/]+\\.(SF|DSA|RSA)");

    private static Path jarPath(final String systemProperty) {
        final String path = System.getProperty(systemProperty);
        assertNotNull("system property " + systemProperty + " was not set - check the maven-failsafe-plugin " +
                "configuration in gremlin-driver/pom.xml", path);
        final Path jar = Paths.get(path);
        assertTrue("jar does not exist at " + jar, Files.isRegularFile(jar));
        return jar;
    }

    @Test
    public void shadedJarShouldHaveNoModuleDescriptor() throws Exception {
        try (JarFile jar = new JarFile(jarPath(DRIVER_SHADED_JAR_PROPERTY).toFile())) {
            assertNull("the -shaded driver jar must not carry a root module-info.class",
                    jar.getEntry("module-info.class"));
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final String name = entries.nextElement().getName();
                assertFalse("the -shaded driver jar must not carry a versioned module-info.class: " + name,
                        VERSIONED_MODULE_INFO.matcher(name).matches());
            }
        }
    }

    @Test
    public void shadedJarShouldHaveNoModuleManifestAttributes() throws Exception {
        try (JarFile jar = new JarFile(jarPath(DRIVER_SHADED_JAR_PROPERTY).toFile())) {
            assertNotNull("the -shaded driver jar must have a manifest", jar.getManifest());
            assertNull("the -shaded driver jar must not declare Automatic-Module-Name",
                    jar.getManifest().getMainAttributes().getValue("Automatic-Module-Name"));
            assertNull("the -shaded driver jar must not declare Multi-Release",
                    jar.getManifest().getMainAttributes().getValue("Multi-Release"));
        }
    }

    @Test
    public void shadedJarShouldHaveNoSignatureFiles() throws Exception {
        try (JarFile jar = new JarFile(jarPath(DRIVER_SHADED_JAR_PROPERTY).toFile())) {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final String name = entries.nextElement().getName();
                assertFalse("the -shaded driver jar must not carry a signature file: " + name,
                        SIGNATURE_FILE.matcher(name).matches());
            }
        }
    }

    @Test
    public void shouldLoadClusterFromShadedJarOnAFreshClassLoader() throws Exception {
        final URL jarUrl = jarPath(DRIVER_SHADED_JAR_PROPERTY).toUri().toURL();
        try (URLClassLoader loader = new URLClassLoader(new URL[]{jarUrl}, ClassLoader.getPlatformClassLoader())) {
            final Class<?> clusterClass = Class.forName("org.apache.tinkerpop.gremlin.driver.Cluster", false, loader);
            assertEquals("org.apache.tinkerpop.gremlin.driver.Cluster", clusterClass.getName());
        }
    }

    @Test
    public void shadedJarShouldNotContainBouncyCastle() throws Exception {
        try (JarFile jar = new JarFile(jarPath(DRIVER_SHADED_JAR_PROPERTY).toFile())) {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final String name = entries.nextElement().getName();
                assertFalse("the -shaded driver jar must not bundle BouncyCastle: " + name,
                        name.startsWith("org/bouncycastle/"));
            }
        }
    }

    @Test
    public void shadedJarShouldRegisterTinkerGraphDefaultGraphProvider() throws Exception {
        try (JarFile jar = new JarFile(jarPath(DRIVER_SHADED_JAR_PROPERTY).toFile())) {
            final JarEntry services = jar.getJarEntry(
                    "META-INF/services/org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider");
            assertNotNull("the -shaded driver jar must register a DefaultGraphProvider service", services);
            try (InputStream in = jar.getInputStream(services)) {
                final String registrations = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                assertTrue("the -shaded driver jar must register TinkerGraph's DefaultGraphProvider: " + registrations,
                        registrations.contains(
                                "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphDefaultProvider"));
            }
        }
    }

    @Test
    public void regularJarShouldHaveExpectedDescriptor() throws Exception {
        final Path jar = jarPath(DRIVER_JAR_PROPERTY);
        final Set<ModuleReference> refs = ModuleFinder.of(jar).findAll();
        assertEquals("expected exactly one module in " + jar, 1, refs.size());
        final ModuleDescriptor descriptor = refs.iterator().next().descriptor();
        assertFalse(jar + " must be an explicit module", descriptor.isAutomatic());
        assertEquals(DRIVER_MODULE, descriptor.name());

        final Set<String> hidden = new TreeSet<>(Arrays.asList(
                "org.apache.tinkerpop.gremlin.driver.handler",
                "org.apache.tinkerpop.gremlin.driver.simple",
                "org.apache.tinkerpop.gremlin.driver.stream",
                "org.apache.tinkerpop.gremlin.driver.util"));
        assertTrue("hidden packages no longer exist: " + hidden, descriptor.packages().containsAll(hidden));
        final Set<String> expectedExports = new TreeSet<>(descriptor.packages());
        expectedExports.removeAll(hidden);
        final Set<String> exports = descriptor.exports().stream().map(ModuleDescriptor.Exports::source).
                collect(Collectors.toCollection(TreeSet::new));
        assertEquals("exports of " + DRIVER_MODULE, expectedExports, exports);
        assertTrue("all exports must be unqualified",
                descriptor.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified));

        assertFalse(DRIVER_MODULE + " must not be an open module", descriptor.isOpen());
        assertTrue("opens of " + DRIVER_MODULE + ": " + descriptor.opens(), descriptor.opens().isEmpty());
        assertTrue("uses of " + DRIVER_MODULE + ": " + descriptor.uses(), descriptor.uses().isEmpty());
        assertServicesMatchProvides(jar, descriptor);
        assertFalse("TinkerGraph is optional for the driver and must not be required: " + descriptor.requires(),
                descriptor.requires().stream().anyMatch(r -> r.name().equals(TINKERGRAPH_MODULE)));
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
