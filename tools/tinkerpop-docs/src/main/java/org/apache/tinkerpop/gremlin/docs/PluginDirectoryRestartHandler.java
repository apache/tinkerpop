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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Physically toggles plugin availability in a Gremlin Console distribution so that conflicting
 * plugins (e.g. Neo4j's Scala 2.11 vs Spark's Scala 2.12) never share one classpath.
 * <p>
 * The console's {@code bin/gremlin.sh} composes its classpath from {@code lib/*.jar} plus
 * {@code ext/<plugin>/plugin/*}. To exclude a plugin this handler moves {@code ext/<plugin>}
 * out to {@code ext-disabled/<plugin>} (off-classpath) and removes its activation class from
 * {@code ext/plugins.txt}; to re-include it the directory is moved back and the class restored.
 * Keeping {@code plugins.txt} in sync is required because the console rewrites that file on
 * startup, permanently dropping any listed plugin whose jars are missing.
 */
final class PluginDirectoryRestartHandler implements ConsoleRestartHandler {

    private static final Logger LOG = Logger.getLogger(PluginDirectoryRestartHandler.class.getName());

    /** Toggleable plugin directory -> activation class written to ext/plugins.txt. */
    private static final Map<String, String> TOGGLEABLE = Collections.unmodifiableMap(new LinkedHashMap<String, String>() {{
        put("neo4j-gremlin", "org.apache.tinkerpop.gremlin.neo4j.jsr223.Neo4jGremlinPlugin");
        put("spark-gremlin", "org.apache.tinkerpop.gremlin.spark.jsr223.SparkGremlinPlugin");
        put("hadoop-gremlin", "org.apache.tinkerpop.gremlin.hadoop.jsr223.HadoopGremlinPlugin");
    }});

    private final Path extDir;
    private final Path disabledDir;
    private final Path pluginsTxt;

    PluginDirectoryRestartHandler(final Path consoleHome) {
        this.extDir = consoleHome.resolve("ext");
        this.disabledDir = consoleHome.resolve("ext-disabled");
        this.pluginsTxt = extDir.resolve("plugins.txt");
    }

    @Override
    public void onRestart(final List<String> excludedPlugins) throws IOException {
        for (final String plugin : TOGGLEABLE.keySet()) {
            if (excludedPlugins.contains(plugin)) {
                disable(plugin);
            } else {
                enable(plugin);
            }
        }
    }

    private void disable(final String plugin) throws IOException {
        final Path active = extDir.resolve(plugin);
        final Path disabled = disabledDir.resolve(plugin);
        if (Files.isDirectory(active)) {
            // The active copy is authoritative. Clear any stale disabled copy left by an
            // interrupted run before moving, since Files.move(REPLACE_EXISTING) cannot replace
            // a non-empty directory.
            Files.createDirectories(disabledDir);
            deleteRecursively(disabled);
            Files.move(active, disabled);
            LOG.info("Excluded plugin: " + plugin);
        }
        setPluginEnabled(TOGGLEABLE.get(plugin), false);
    }

    private void enable(final String plugin) throws IOException {
        final Path active = extDir.resolve(plugin);
        final Path disabled = disabledDir.resolve(plugin);
        if (Files.isDirectory(disabled)) {
            if (Files.isDirectory(active)) {
                // Plugin is already present in ext/ (e.g. freshly installed); the active copy
                // wins. Just drop the leftover disabled duplicate.
                deleteRecursively(disabled);
            } else {
                Files.move(disabled, active);
                LOG.info("Restored plugin: " + plugin);
            }
        }
        setPluginEnabled(TOGGLEABLE.get(plugin), true);
    }

    /** Recursively deletes a directory tree if it exists; a no-op otherwise. */
    private static void deleteRecursively(final Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (java.util.stream.Stream<Path> walk = Files.walk(path)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (final IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } catch (final java.io.UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Adds or removes a single activation class line in ext/plugins.txt, preserving the rest. */
    private void setPluginEnabled(final String pluginClass, final boolean enabled) throws IOException {
        if (!Files.exists(pluginsTxt)) return;
        final List<String> lines = Files.readAllLines(pluginsTxt).stream()
                .filter(l -> !l.trim().equals(pluginClass))
                .collect(Collectors.toList());
        if (enabled) lines.add(pluginClass);
        Files.write(pluginsTxt, lines);
    }
}
