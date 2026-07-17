/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.docs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link PluginDirectoryRestartHandler} verifying that toggling plugin
 * directories is idempotent and resilient to stale {@code ext-disabled/} state left by an
 * interrupted build.
 */
public class PluginDirectoryRestartHandlerTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String SPARK = "spark-gremlin";
    private static final String SPARK_CLASS = "org.apache.tinkerpop.gremlin.spark.jsr223.SparkGremlinPlugin";

    private Path consoleHome;
    private Path ext;
    private Path disabled;
    private Path pluginsTxt;
    private PluginDirectoryRestartHandler handler;

    @Before
    public void setUp() throws IOException {
        consoleHome = tmp.getRoot().toPath();
        ext = Files.createDirectories(consoleHome.resolve("ext"));
        disabled = consoleHome.resolve("ext-disabled");
        pluginsTxt = ext.resolve("plugins.txt");
        // Seed a populated plugin layout with a non-empty plugin dir for each toggleable plugin.
        for (final String p : Arrays.asList(SPARK, "hadoop-gremlin")) {
            installPlugin(p);
        }
        Files.write(pluginsTxt, Arrays.asList(
                "org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin",
                SPARK_CLASS,
                "org.apache.tinkerpop.gremlin.hadoop.jsr223.HadoopGremlinPlugin"));
        handler = new PluginDirectoryRestartHandler(consoleHome);
    }

    private void installPlugin(final String plugin) throws IOException {
        final Path dir = Files.createDirectories(ext.resolve(plugin).resolve("plugin"));
        Files.write(dir.resolve(plugin + ".jar"), "jar-bytes".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldDisableAndReEnablePlugin() throws IOException {
        handler.onRestart(Collections.singletonList(SPARK));
        assertThat(Files.isDirectory(ext.resolve(SPARK)), is(false));
        assertThat(Files.isDirectory(disabled.resolve(SPARK)), is(true));
        assertThat(pluginClasses().contains(SPARK_CLASS), is(false));

        // A later book with no exclusions restores everything.
        handler.onRestart(Collections.emptyList());
        assertThat(Files.isDirectory(ext.resolve(SPARK)), is(true));
        assertThat(Files.isDirectory(disabled.resolve(SPARK)), is(false));
        assertThat(pluginClasses().contains(SPARK_CLASS), is(true));
        // The plugin jar survived the round trip.
        assertThat(Files.exists(ext.resolve(SPARK).resolve("plugin").resolve(SPARK + ".jar")), is(true));
    }

    @Test
    public void shouldBeIdempotentWhenExcludingTwice() throws IOException {
        handler.onRestart(Collections.singletonList("hadoop-gremlin"));
        // Excluding the same plugin again must not throw, even though ext-disabled/hadoop-gremlin already exists.
        handler.onRestart(Collections.singletonList("hadoop-gremlin"));
        assertThat(Files.isDirectory(ext.resolve("hadoop-gremlin")), is(false));
        assertThat(Files.isDirectory(disabled.resolve("hadoop-gremlin")), is(true));
    }

    @Test
    public void shouldRecoverFromStaleDisabledDirectoryLeftByInterruptedRun() throws IOException {
        // Simulate a crashed prior run: a non-empty ext-disabled/hadoop-gremlin exists AND
        // ext/hadoop-gremlin was re-installed by process-docs.sh, so the plugin is present in
        // BOTH locations.
        Files.createDirectories(disabled.resolve("hadoop-gremlin").resolve("plugin"));
        Files.write(disabled.resolve("hadoop-gremlin").resolve("plugin").resolve("stale.jar"),
                "stale".getBytes(StandardCharsets.UTF_8));

        // Disabling must not throw (the move target already exists and is non-empty).
        handler.onRestart(Collections.singletonList("hadoop-gremlin"));
        assertThat(Files.isDirectory(ext.resolve("hadoop-gremlin")), is(false));
        assertThat(Files.isDirectory(disabled.resolve("hadoop-gremlin")), is(true));
        // The authoritative active copy replaced the stale one (no stale.jar remains).
        assertThat(Files.exists(disabled.resolve("hadoop-gremlin").resolve("plugin").resolve("stale.jar")), is(false));
        assertThat(Files.exists(disabled.resolve("hadoop-gremlin").resolve("plugin").resolve("hadoop-gremlin.jar")), is(true));
    }

    @Test
    public void shouldEnableCleanlyWhenPluginPresentInBothLocations() throws IOException {
        // ext-disabled/spark left over AND ext/spark freshly installed: enabling drops the duplicate.
        Files.createDirectories(disabled.resolve(SPARK).resolve("plugin"));
        Files.write(disabled.resolve(SPARK).resolve("plugin").resolve("stale.jar"),
                "stale".getBytes(StandardCharsets.UTF_8));

        handler.onRestart(Collections.emptyList());
        assertThat(Files.isDirectory(ext.resolve(SPARK)), is(true));
        assertThat(Files.isDirectory(disabled.resolve(SPARK)), is(false));
        assertThat(pluginClasses().contains(SPARK_CLASS), is(true));
    }

    private List<String> pluginClasses() throws IOException {
        return Files.readAllLines(pluginsTxt);
    }
}
