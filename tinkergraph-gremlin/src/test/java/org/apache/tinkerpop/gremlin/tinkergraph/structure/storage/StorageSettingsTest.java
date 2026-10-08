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
import java.nio.file.Files;
import java.util.Iterator;

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
        changed.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_PRESERVE_VP_IDS, true);
        graph = TinkerStorageGraph.open(changed);
        assertEquals(1, countOf(graph.vertices()));
        graph.close();

        assertEquals("the record is written once and never rewritten",
                new String(recorded), new String(Files.readAllBytes(settingsFile().toPath())));
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
