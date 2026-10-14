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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the settings a {@link TinkerStorageGraph} store records when it is created and holds fixed after that.
 */
public class StorageSettingsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;

    @Before
    public void setUp() throws Exception {
        location = tempFolder.newFolder("storage").getAbsolutePath();
    }

    @Test
    public void shouldAdoptTheRecordedSettingsWhenTheyAreNotConfigured() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.LONG.name());
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY,
                VertexProperty.Cardinality.list.name());
        TinkerStorageGraph graph = TinkerStorageGraph.open(created);
        graph.addVertex(T.id, 1L, "name", "a", "name", "b");
        graph.tx().commit();
        graph.close();

        // only the engine and directory: everything else comes from the store
        graph = TinkerStorageGraph.open(config());
        assertEquals(TinkerGraph.DefaultIdManager.LONG.name(),
                graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER));
        assertEquals("list cardinality keeps both values", 2,
                countOf(graph.vertices(1L).next().properties("name")));
        final Vertex added = graph.addVertex();
        assertTrue("new ids come from the recorded LONG manager", added.id() instanceof Long);
        graph.tx().rollback();
        graph.close();
    }

    @Test
    public void shouldFailToOpenWithADifferentFixedSetting() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.LONG.name());
        TinkerStorageGraph.open(created).close();

        final Configuration changed = config();
        changed.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.UUID.name());
        try {
            TinkerStorageGraph.open(changed).close();
            fail("a store must not open with a different id manager than it was created with");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("vertexIdManager=LONG"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("g.io()"));
        }

        // the failed open released the directory
        TinkerStorageGraph.open(config()).close();
    }

    @Test
    public void shouldTreatSpellingsOfTheSameSettingAlike() {
        TinkerStorageGraph.open(config()).close();

        final Configuration respelled = config();
        respelled.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "GRAPHBINARY");
        respelled.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, "FALSE");
        respelled.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        TinkerStorageGraph.open(respelled).close();
    }

    @Test
    public void shouldAllowOperationalSettingsToChange() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();
        final byte[] recorded = Files.readAllBytes(settingsFile().toPath());

        final Configuration changed = config();
        changed.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, "os");
        changed.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 0);
        graph = TinkerStorageGraph.open(changed);
        assertEquals(1, countOf(graph.vertices()));
        graph.close();

        assertEquals("the record is written once and never rewritten",
                new String(recorded), new String(Files.readAllBytes(settingsFile().toPath())));
    }

    @Test
    public void shouldKeepPreservedVertexPropertyIdsWhenTheSettingIsLeftOut() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS, true);
        TinkerStorageGraph graph = TinkerStorageGraph.open(created);
        graph.addVertex(T.id, 1).property("name", "marko", T.id, "vp-explicit");
        graph.tx().commit();
        graph.close();

        // an open that leaves the setting out adopts the recorded one, so its close-time compaction keeps the ids
        graph = TinkerStorageGraph.open(config());
        assertTrue(graph.configuration().getBoolean(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS));
        graph.close();

        graph = TinkerStorageGraph.open(config());
        assertEquals("vp-explicit", graph.vertices(1).next().properties("name").next().id());
        graph.close();
    }

    @Test
    public void shouldFailToOpenWithADifferentPreserveVertexPropertyIdsSetting() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS, true);
        TinkerStorageGraph.open(created).close();

        final Configuration changed = config();
        changed.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS, false);
        try {
            TinkerStorageGraph.open(changed).close();
            fail("a store that preserves vertex-property ids must not open with the setting turned off");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("preserveVertexPropertyIds=true"));
        }

        // spellings Configuration.getBoolean reads as true name the recorded setting
        final Configuration yes = config();
        yes.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS, "yes");
        TinkerStorageGraph.open(yes).close();
    }

    @Test
    public void shouldOpenAStoreRegardlessOfTheDefaultLocale() {
        // a Turkish default locale upper-cases the "i" of graphbinary to a dotted capital I, which names no engine
        final Locale original = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try {
            TinkerStorageGraph.open(config()).close();
            TinkerStorageGraph.open(config()).close();
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void shouldReadBackARecordedSettingThatIsNotLatin1() {
        // the record is written as UTF-8, so it must be read as UTF-8 rather than the ISO-8859-1 Properties assumes
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, "com.example.\u03a9IdManager");
        final File directory = new File(location);
        StorageSettings.reconcile(created, directory);
        assertEquals("com.example.\u03a9IdManager", StorageSettings.reconcile(created, directory)
                .getString(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER));
    }

    @Test
    public void shouldNotModifyTheSuppliedConfiguration() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.LONG.name());
        TinkerStorageGraph.open(created).close();

        final Configuration reopened = config();
        TinkerStorageGraph.open(reopened).close();
        assertFalse(reopened.containsKey(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER));
    }

    @Test
    public void shouldFailWhenTheSettingsRecordIsMissing() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();
        assertTrue(settingsFile().delete());

        try {
            TinkerStorageGraph.open(config()).close();
            fail("a store with data but no settings record should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(StorageSettings.SETTINGS_FILE));
        }

        final Configuration recover = config();
        recover.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, true);
        graph = TinkerStorageGraph.open(recover);
        assertEquals(1, countOf(graph.vertices()));
        graph.close();
        assertFalse("recovery must not write the record", settingsFile().exists());
    }

    @Test
    public void shouldFailWhenTheSettingsRecordLacksAFixedSetting() throws Exception {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
        TinkerStorageGraph graph = TinkerStorageGraph.open(created);
        graph.addVertex(T.id, 1, "a", 1, "a", 2);
        graph.tx().commit();
        graph.close();

        // a damaged record that lost a line must not silently fall back to the default cardinality, which would keep
        // only the last value of each multi-property and write that back at the next compaction
        final String key = TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY;
        final StringBuilder kept = new StringBuilder();
        for (final String line : Files.readAllLines(settingsFile().toPath(), StandardCharsets.ISO_8859_1)) {
            if (!line.startsWith(key))
                kept.append(line).append('\n');
        }
        Files.write(settingsFile().toPath(), kept.toString().getBytes(StandardCharsets.ISO_8859_1));
        final byte[] damaged = Files.readAllBytes(settingsFile().toPath());

        try {
            TinkerStorageGraph.open(config()).close();
            fail("a settings record missing a fixed setting should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("do not record " + key));
        }

        final Configuration recover = config();
        recover.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, true);
        recover.setProperty(key, "list");
        graph = TinkerStorageGraph.open(recover);
        assertEquals(2, countOf(graph.vertices(1).next().properties("a")));
        graph.close();
        assertTrue("recovery must not rewrite the record",
                Arrays.equals(damaged, Files.readAllBytes(settingsFile().toPath())));
    }

    @Test
    public void shouldNotRecordAnUnusableSettingForANewStore() {
        final Configuration wrong = config();
        wrong.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "LIST");
        try {
            TinkerStorageGraph.open(wrong).close();
            fail("an unknown cardinality should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("must be one of"));
        }
        assertFalse("a failed first open must not record its settings", settingsFile().exists());

        final Configuration wrongIds = config();
        wrongIds.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, "long");
        try {
            TinkerStorageGraph.open(wrongIds).close();
            fail("an unknown id manager should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("id manager with long"));
        }
        assertFalse("a failed first open must not record its settings", settingsFile().exists());

        // the corrected value then creates the store as a first open
        final Configuration right = config();
        right.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
        TinkerStorageGraph.open(right).close();
        final TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        assertEquals("list", graph.configuration().getString(
                TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY));
        graph.close();
    }

    @Test
    public void shouldReadAllowNullPropertyValuesAsConfigurationDoes() {
        final Configuration created = config();
        created.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
        TinkerStorageGraph.open(created).close();

        // Configuration.getBoolean reads "yes" as true, so it names the recorded setting
        final Configuration yes = config();
        yes.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, "yes");
        TinkerStorageGraph.open(yes).close();

        final Configuration unreadable = config();
        unreadable.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, "maybe");
        try {
            TinkerStorageGraph.open(unreadable).close();
            fail("a value that is neither true nor false must not match the record");
        } catch (IllegalStateException expected) {
            assertTrue("the error should give the configured value: " + expected.getMessage(),
                    expected.getMessage().contains("cannot be opened with maybe"));
        }
    }

    @Test
    public void shouldNameTheSettingsFileWhenItIsMalformed() throws Exception {
        TinkerStorageGraph.open(config()).close();
        // a truncated unicode escape, which Properties.load rejects
        Files.write(settingsFile().toPath(),
                "gremlin.tinkergraph.vertexIdManager=\\u12".getBytes(StandardCharsets.ISO_8859_1));
        try {
            TinkerStorageGraph.open(config()).close();
            fail("a malformed settings record should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(settingsFile().getPath()));
        }
    }

    @Test
    public void shouldLeaveNoLockFileInADirectoryThatIsNotAStore() throws Exception {
        final File foreign = new File(location);
        Files.write(new File(foreign, "notes.txt").toPath(), "not a store".getBytes(StandardCharsets.UTF_8));
        try {
            TinkerStorageGraph.open(config()).close();
            fail("a directory holding other files should not open as a store");
        } catch (IllegalStateException expected) {
            // the directory holds no storage files, so pointing at recovery would mislead
            assertTrue(expected.getMessage(), expected.getMessage().contains("is not empty and is not a store"));
        }
        assertEquals(Collections.singletonList("notes.txt"), Arrays.asList(foreign.list()));
    }

    @Test
    public void shouldSayWhyTheStorageDirectoryCannotBeCreated() throws Exception {
        final File file = tempFolder.newFile("plain-file");
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, new File(file, "store").getPath());
        try {
            TinkerStorageGraph.open(conf).close();
            fail("a directory under a regular file cannot be created");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Could not create storage directory"));
            assertTrue("the cause should say why: " + expected.getMessage(), null != expected.getCause());
        }
    }

    @Test
    public void shouldTrimTheStorageEngineName() {
        final Configuration padded = config();
        padded.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, " graphbinary ");
        TinkerStorageGraph.open(padded).close();
    }

    @Test
    public void shouldListTheBuiltInEnginesForAnUnknownEngine() {
        final Configuration unknown = config();
        unknown.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "rocksdb");
        try {
            TinkerStorageGraph.open(unknown).close();
            fail("an unknown storage engine should not open");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("'rocksdb'"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("graphbinary"));
        }
    }

    @Test
    public void shouldNotCreateADirectoryToRecover() throws Exception {
        final File missing = new File(location, "missing");
        final Configuration recover = config();
        recover.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, missing.getAbsolutePath());
        recover.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, true);
        try {
            TinkerStorageGraph.open(recover).close();
            fail("recovery of a directory that does not exist should fail");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(missing.getPath()));
        }
        assertFalse("recovery must not create the directory", missing.exists());
    }

    @Test
    public void shouldNotLeaveALockFileAfterRecovery() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();
        // a store copied without its LOCK file, as a backup might be
        Files.delete(new File(location, DirectoryLock.LOCK_FILE).toPath());

        final Configuration recover = config();
        recover.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, true);
        graph = TinkerStorageGraph.open(recover);
        assertEquals(1, countOf(graph.vertices()));
        graph.close();
        assertFalse("recovery must leave the directory as it found it",
                new File(location, DirectoryLock.LOCK_FILE).exists());
    }

    private Configuration config() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "graphbinary");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
        return conf;
    }

    private File settingsFile() {
        return new File(location, StorageSettings.SETTINGS_FILE);
    }

    private static int countOf(final Iterator<?> it) {
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }
}
