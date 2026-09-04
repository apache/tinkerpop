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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies that a {@link TinkerStorageGraph} takes an exclusive lock on its storage directory so a second opener on
 * the same location fails fast rather than corrupting the store.
 */
public class DirectoryLockTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;

    @Before
    public void setUp() throws Exception {
        location = tempFolder.newFolder("storage").getAbsolutePath();
    }

    private Configuration config() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "graphbinary");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
        return conf;
    }

    @Test
    public void shouldReleaseLockOnCloseAndAllowReopen() {
        // sequential open/close must not leave the directory wedged
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(config());
        assertNotNull(graph.vertices(1).next());
        graph.close();
    }

    @Test
    public void shouldRejectSecondOpenWhileLocationIsLocked() throws Exception {
        // simulate another process holding the directory lock by taking the OS lock on the LOCK file directly
        final File dir = new File(location);
        dir.mkdirs();
        final File lockFile = new File(dir, DirectoryLock.LOCK_FILE);
        try (final FileChannel channel = FileChannel.open(lockFile.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             final FileLock held = channel.lock()) {
            assertNotNull(held);
            try {
                TinkerStorageGraph.open(config());
                fail("expected open to fail while the storage location is locked");
            } catch (IllegalStateException expected) {
                assertTrue("message should name the location: " + expected.getMessage(),
                        expected.getMessage().contains(location));
            }
        }

        // once the simulated holder releases (try-with-resources above), the location opens normally
        final TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.close();
    }
}
