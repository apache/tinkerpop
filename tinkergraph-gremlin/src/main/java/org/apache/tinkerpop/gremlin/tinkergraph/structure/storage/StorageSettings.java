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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.structure.LabelCardinality;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The settings a storage directory was created with, recorded in {@link #SETTINGS_FILE} beside the engine's own files.
 * <p/>
 * Some settings shape the data itself: the id managers decide the type of every stored id, and the cardinalities and
 * null handling decide how stored properties read back. A store is only ever read with the settings that wrote it, so
 * these are fixed when the store is created. Changing one is a migration: export the graph with {@code g.io()} and
 * import it into a new store. The record is written once, holding the effective value of each such setting (defaults
 * included), and never rewritten.
 * <p/>
 * On open, a setting the configuration leaves unset takes the recorded value, so a store opens with nothing but its
 * storage engine and directory and the original configuration can be lost without harm. A setting that is set to a
 * different value fails the open. Settings that only affect how the engine runs, such as the sync mode or compaction
 * threshold, are not recorded and may change freely.
 */
public final class StorageSettings {

    private static final Logger logger = LoggerFactory.getLogger(StorageSettings.class);

    public static final String SETTINGS_FILE = "settings.properties";

    /**
     * The settings fixed at creation, each with the value TinkerGraph uses when it is not configured.
     */
    private static final Map<String, String> FIXED;

    static {
        final Map<String, String> fixed = new LinkedHashMap<>();
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, null);
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY,
                VertexProperty.Cardinality.single.name());
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, LabelCardinality.ONE.name());
        fixed.put(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, Boolean.FALSE.toString());
        FIXED = Collections.unmodifiableMap(fixed);
    }

    private StorageSettings() {
    }

    /**
     * Reconcile {@code config} with the settings recorded in {@code directory}, returning the configuration the graph
     * should use. The supplied configuration is not modified. A new store records the effective settings now. An
     * existing one fills in each unset fixed setting from the record and fails on any that differ. A recovery open
     * changes nothing on disk and reads with the recorded settings, warning where the configuration disagrees.
     *
     * @param config    the configuration the graph was opened with
     * @param directory the storage directory, already locked by the opening graph
     * @return a copy of {@code config} with the store's fixed settings applied
     */
    public static Configuration reconcile(final Configuration config, final File directory) {
        final boolean recovering = config.getBoolean(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, false);
        final Configuration effective = new BaseConfiguration();
        ConfigurationUtils.copy(config, effective);

        final File file = new File(directory, SETTINGS_FILE);
        if (!file.isFile()) {
            if (isNewStore(directory)) {
                if (!recovering)
                    write(file, fixedSettingsOf(effective));
            } else if (!recovering) {
                throw new IllegalStateException(String.format(
                        "Storage at %s has data but no %s recording the settings it was created with; open it with %s " +
                                "to read it with the configured settings",
                        directory, SETTINGS_FILE, TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            } else {
                logger.warn("Storage at {} has no {}; reading it with the configured settings",
                        directory, SETTINGS_FILE);
            }
            return effective;
        }

        final Map<String, String> recorded = read(file);
        for (final Map.Entry<String, String> setting : FIXED.entrySet()) {
            final String key = setting.getKey();
            final String recordedValue = recorded.get(key);
            if (null == recordedValue)
                continue;
            if (effective.containsKey(key)) {
                final String configured = normalize(key, effective.getString(key));
                if (configured.equals(recordedValue))
                    continue;
                if (!recovering)
                    throw new IllegalStateException(String.format(
                            "Storage at %s was created with %s=%s and cannot be opened with %s. To change it, export " +
                                    "the graph with g.io() and import it into a new storage directory",
                            directory, key, recordedValue, configured));
                logger.warn("Storage at {} was created with {}={}; reading it with that rather than the configured {}",
                        directory, key, recordedValue, configured);
            }
            effective.setProperty(key, recordedValue);
        }
        return effective;
    }

    private static boolean isNewStore(final File directory) {
        final String[] names = directory.list();
        return null == names || Arrays.stream(names).allMatch(name ->
                name.equals(DirectoryLock.LOCK_FILE) || name.equals(SETTINGS_FILE + ".tmp"));
    }

    private static Map<String, String> fixedSettingsOf(final Configuration config) {
        final Map<String, String> settings = new LinkedHashMap<>();
        for (final Map.Entry<String, String> setting : FIXED.entrySet()) {
            final String value = config.containsKey(setting.getKey()) ?
                    normalize(setting.getKey(), config.getString(setting.getKey())) : setting.getValue();
            if (value != null)
                settings.put(setting.getKey(), value);
        }
        return settings;
    }

    /**
     * Put a configured value in the form it is recorded in, so that spellings TinkerGraph treats alike compare equal.
     */
    private static String normalize(final String key, final String value) {
        final String trimmed = value.trim();
        if (key.equals(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE)) {
            for (final DefaultStorage engine : DefaultStorage.values()) {
                if (engine.name().equalsIgnoreCase(trimmed))
                    return engine.name().toLowerCase();
            }
        } else if (key.equals(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES)) {
            return Boolean.toString(Boolean.parseBoolean(trimmed));
        }
        return trimmed;
    }

    private static Map<String, String> read(final File file) {
        final Properties properties = new Properties();
        try (final InputStream in = new FileInputStream(file)) {
            properties.load(in);
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage settings %s", file), ex);
        }
        final Map<String, String> settings = new LinkedHashMap<>();
        for (final String key : properties.stringPropertyNames())
            settings.put(key, properties.getProperty(key));
        return settings;
    }

    private static void write(final File file, final Map<String, String> settings) {
        final Properties properties = new Properties();
        properties.putAll(settings);
        final File tmp = new File(file.getParentFile(), SETTINGS_FILE + ".tmp");
        try {
            try (final FileOutputStream fos = new FileOutputStream(tmp);
                 final Writer out = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
                properties.store(out, "Settings this TinkerStorageGraph store was created with. They cannot be " +
                        "changed; export and import the graph to use different ones.");
                out.flush();
                fos.getFD().sync();
            }
            StorageFiles.atomicMove(tmp, file);
            StorageFiles.syncDirectory(file.getParentFile());
        } catch (IOException ex) {
            StorageFiles.deleteQuietly(tmp);
            throw new UncheckedIOException(String.format("Could not record storage settings %s", file), ex);
        }
    }
}
