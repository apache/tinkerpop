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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Deterministically verifies that {@link TinkerStorageGraph} serializes the durable write of concurrent commits.
 * Because TinkerGraph transactions lock only their own changed elements, commits touching disjoint elements run their
 * commit paths concurrently; the storage engine's single append log would interleave (corrupt) without the
 * commit-write lock. Rather than rely on a race actually corrupting the log, this drives commits through
 * {@link ConcurrencyProbeStorage}, which records whether two threads are ever inside {@code persist()} at once. With
 * the lock that count is exactly zero; without it the probe trips reliably.
 */
public class StorageCommitSerializationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;

    @Before
    public void setUp() throws Exception {
        location = tempFolder.newFolder("storage").getAbsolutePath();
        ConcurrencyProbeStorage.reset();
    }

    private TinkerStorageGraph open() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, ConcurrencyProbeStorage.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
        return TinkerStorageGraph.open(conf);
    }

    @Test
    public void shouldSerializeConcurrentCommitWrites() throws Exception {
        final int threads = 8;
        final int commitsPerThread = 20;
        final TinkerStorageGraph graph = open();
        try {
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < commitsPerThread; i++) {
                        graph.addVertex(T.id, threadId * commitsPerThread + i);
                        graph.tx().commit();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        } finally {
            graph.close();
        }

        // the lock must have kept persist() strictly single-threaded
        assertEquals("commits entered persist() concurrently", 0, ConcurrencyProbeStorage.concurrentEntries.get());
        assertEquals("more than one thread was inside persist() at once", 1, ConcurrencyProbeStorage.maxObserved.get());
    }
}
