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
package org.apache.tinkerpop.gremlin.server;

import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SettingsTest {
    private Path tempDir;

    @Before
    public void before() throws Exception {
        tempDir = Files.createTempDirectory(SettingsTest.class.getSimpleName());
    }

    @After
    public void afterClass() throws Exception {
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    private static class CustomSettings extends Settings {
        public String customValue = "localhost";

        public static CustomSettings read(final InputStream stream) {
            final Constructor constructor = createDefaultYamlConstructor();
            final Yaml yaml = new Yaml(constructor);
            return yaml.loadAs(stream, CustomSettings.class);
        }
    }

    @Test
    public void constructorCanBeExtendToParseCustomYamlAndSettingsValues() {
        final InputStream stream = SettingsTest.class.getResourceAsStream("custom-gremlin-server.yaml");

        final CustomSettings settings = CustomSettings.read(stream);

        assertEquals("hello", settings.customValue);
        assertEquals("remote", settings.host);
    }

    @Test
    public void defaultCustomValuesAreHandledCorrectly() {
        final InputStream stream = SettingsTest.class.getResourceAsStream("gremlin-server-integration.yaml");

        final CustomSettings settings = CustomSettings.read(stream);

        assertEquals("localhost", settings.customValue);
    }

    @Test
    public void testSimpleIncludeAndMerge() throws IOException {
        createFile("base.yaml", "graphs: { graph1: 'base1', graph2: 'base2' }");
        createFile("root.yaml",
                "includes: ['base.yaml']\n" +
                        "graphs:\n" +
                        "  graph1: 'root'"

        );

        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());

        assertEquals("root", result.graphs.get("graph1"));
        assertEquals("base2", result.graphs.get("graph2"));
    }

    @Test
    public void testDeepMerge() throws IOException {
        createFile("base.yaml",
                "scriptEngines:\n" +
                        "  engine1:\n" +
                        "    config: \n" +
                        "      connector: { port: 8080, protocol: 'http' }\n" +
                        "      logging: { level: 'INFO' }"
        );
        createFile("root.yaml",
                "includes: ['base.yaml']\n" +
                        "scriptEngines:\n" +
                        "  engine1:\n" +
                        "    config: \n" +
                        "      connector: { port: 9090 }\n" +
                        "      logging: { file: '/var/log/app.log' }"
        );


        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());
        @SuppressWarnings("unchecked")
        Map<String, Object> connector = (Map<String, Object>) result.scriptEngines.get("engine1").config.get("connector");
        @SuppressWarnings("unchecked")
        Map<String, Object> logging = (Map<String, Object>) result.scriptEngines.get("engine1").config.get("logging");

        assertEquals(9090, connector.get("port"));
        assertEquals("http", connector.get("protocol"));
        assertEquals("INFO", logging.get("level"));
        assertEquals("/var/log/app.log", logging.get("file"));
    }

    @Test
    public void testMultipleIncludesOrder() throws IOException {
        // Arrange
        createFile("one.yaml", "host: localhost1");
        createFile("two.yaml", "host: localhost2");
        createFile("root.yaml",
                "includes: ['one.yaml', 'two.yaml']\n" // two should overwrite one
        );

        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());

        assertEquals("localhost2", result.host);
    }

    @Test
    public void testListReplacement() throws IOException {
        createFile("base.yaml", "serializers: \n" +
                "  [ {className: 'a'} , {className: 'b' }]");
        createFile("root.yaml",
                "includes: ['base.yaml']\n" +
                        "serializers: \n" +
                        "  [ {className: 'c'}]");
        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());

        List<Settings.SerializerSettings> serializers = result.serializers;
        assertEquals(1, serializers.size());
        assertEquals("c", serializers.get(0).className);
    }

    @Test
    public void testRelativePathResolution() throws IOException {
        // Structure:
        // root.yaml
        // subdir/
        //   child.yaml
        //   nested/
        //     grandchild.yaml

        createFile("subdir/nested/grandchild.yaml", "host: 0.0.0.0");
        createFile("subdir/child.yaml",
                "includes: ['nested/grandchild.yaml']\n" +
                        "port: 9090"
        );
        createFile("root.yaml",
                "includes: ['subdir/child.yaml']\n" +
                        "threadPoolWorker: 1000"
        );

        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());
        assertEquals(9090, result.port);
        assertEquals(1000, result.threadPoolWorker);
        assertEquals("0.0.0.0", result.host);
    }

    @Test
    public void testParentPathResolution() throws IOException {
        // root.yaml -> includes 'config/sub.yaml'
        // config/sub.yaml -> includes '../shared.yaml'

        createFile("shared.yaml", "host: 0.0.0.0");
        createFile("config/sub.yaml", "includes: ['../shared.yaml']");
        createFile("root.yaml", "includes: ['config/sub.yaml']");

        Settings result = Settings.read(tempDir.resolve("root.yaml").toString());

        assertEquals("0.0.0.0", result.host);
    }

    @Test
    public void testCircularDependency() throws IOException {
        createFile("a.yaml", "includes: ['b.yaml']");
        createFile("b.yaml", "includes: ['a.yaml']");

        try {
            Settings.read(tempDir.resolve("a.yaml").toString());
            Assert.fail("Expected exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Circular dependency detected"));
        }
    }

    @Test
    public void testDiamondDependency() throws IOException {
        // A -> B, A -> C
        // B -> D
        // C -> D
        // This is valid. D should be loaded twice (or handled gracefully) but not cause a cycle error.

        createFile("d.yaml", "host: '0.0.0.0'");
        createFile("b.yaml", "includes: ['d.yaml']\nport: 9090");
        createFile("c.yaml", "includes: ['d.yaml']\nthreadPoolWorker: 1000");
        createFile("a.yaml", "includes: ['b.yaml', 'c.yaml']");

        Settings result = Settings.read(tempDir.resolve("a.yaml").toString());
        assertEquals(9090, result.port);
        assertEquals(1000, result.threadPoolWorker);
        assertEquals("0.0.0.0", result.host);
    }

    @Test
    public void testClasspathResolution() {
        Settings result = Settings.read("classpath:org/apache/tinkerpop/gremlin/server/settings/config/root.yaml");

        assertEquals(9090, result.port);
        assertEquals(1000, result.threadPoolWorker);
        assertEquals("localhost1", result.host);
    }

    @Test
    public void testFileSystemIncludingClasspath() throws IOException {
        createFile("disk.yaml", "includes: ['classpath:org/apache/tinkerpop/gremlin/server/settings/config/root.yaml']\ngremlinPool: 2000");
        Settings result = Settings.read(tempDir.resolve("disk.yaml").toString());

        assertEquals(9090, result.port);
        assertEquals(1000, result.threadPoolWorker);
        assertEquals("localhost1", result.host);
        assertEquals(2000, result.gremlinPool);
    }

    @Test
    public void testMissingFile() {
        try {
            Settings.read(tempDir.resolve("root.yaml").toString());
            Assert.fail("Expected exception");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("root.yaml"));
            assertTrue(msg.contains("Error loading YAML from"));
        }
    }

    @Test
    public void testMissingIncludedFile() throws IOException {
        createFile("root.yaml", "includes: ['non_existent.yaml']");
        try {
            Settings.read(tempDir.resolve("root.yaml").toString());
            Assert.fail("Expected exception");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("non_existent.yaml"));
            assertTrue(msg.contains("Error loading YAML from"));
        }
    }

    @Test
    public void testMalformedIncludes() throws IOException {
        createFile("root.yaml", "includes: 'just_a_string.yaml'");

        try {
            Settings.read(tempDir.resolve("root.yaml").toString());
            Assert.fail("Expected exception");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("'includes' must be a list of strings"));
        }
    }

    @Test
    public void testEmptyFile() throws IOException {
        createFile("empty.yaml", "");
        Settings settings = Settings.read(tempDir.resolve("empty.yaml").toString());
        assertNotNull(settings);
    }

    private void createFile(String fileName, String content) throws IOException {
        Path file = tempDir.resolve(fileName);
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }
}
