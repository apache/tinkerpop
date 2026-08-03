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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TinkerStorage} test double that deterministically detects whether the commit path serializes writes. It
 * persists nothing; instead {@link #persist} tracks how many threads are inside it at once and records a violation if
 * ever more than one is. It also sleeps briefly while "inside" to widen the window, so an unserialized commit path
 * trips the detector reliably rather than racily. Selected by fully-qualified class name via
 * {@code gremlin.tinkergraph.storage}. State is static because the engine is instantiated reflectively.
 */
public final class ConcurrencyProbeStorage implements TinkerStorage {

    static final AtomicInteger inFlight = new AtomicInteger(0);
    static final AtomicInteger maxObserved = new AtomicInteger(0);
    static final AtomicInteger concurrentEntries = new AtomicInteger(0);

    static void reset() {
        inFlight.set(0);
        maxObserved.set(0);
        concurrentEntries.set(0);
    }

    @Override
    public void open(final AbstractTinkerGraph graph, final Configuration config) { }

    @Override
    public void replay(final AbstractTinkerGraph graph) { }

    @Override
    public void persist(final long txVersion,
                        final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                        final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) {
        final int concurrent = inFlight.incrementAndGet();
        try {
            maxObserved.accumulateAndGet(concurrent, Math::max);
            if (concurrent > 1)
                concurrentEntries.incrementAndGet();
            // widen the window so an unserialized path is caught deterministically, not by luck
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void flush() { }

    @Override
    public void compact(final AbstractTinkerGraph graph) { }

    @Override
    public void close() { }
}
