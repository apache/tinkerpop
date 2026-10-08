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
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TinkerStorage} test double that writes the real {@code graphbinary} format but can be told to fail. While
 * {@link #failLogWrites} is set, every write that reaches the log file throws, as a failing disk would. Small frames
 * are buffered, so for them the failure surfaces at flush; a frame larger than the buffer fails during the append
 * itself. While {@link #failCompaction} is set, compaction throws before touching any file, and while
 * {@link #failSnapshotWrites} is set it fails partway through writing the new snapshot. {@link #compactionAttempts}
 * counts every compaction started, whether or not it fails. Selected by
 * fully-qualified class name via {@code gremlin.tinkergraph.storage}. The switches are static because the engine is
 * instantiated reflectively.
 */
public final class FaultInjectingStorage extends AbstractLogStorage {

    static volatile boolean failLogWrites = false;
    static volatile boolean failCompaction = false;
    static volatile boolean failSnapshotWrites = false;
    static final AtomicInteger compactionAttempts = new AtomicInteger();

    static void reset() {
        failLogWrites = false;
        failCompaction = false;
        failSnapshotWrites = false;
        compactionAttempts.set(0);
    }

    private final GraphBinaryStorage codec = new GraphBinaryStorage();

    @Override
    protected FileOutputStream openLogForAppend(final File file) throws IOException {
        return new FileOutputStream(file, true) {
            @Override
            public void write(final int b) throws IOException {
                failIfArmed();
                super.write(b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                failIfArmed();
                super.write(b, off, len);
            }

            private void failIfArmed() throws IOException {
                if (failLogWrites)
                    throw new IOException("injected log write failure");
            }
        };
    }

    @Override
    public void compact(final AbstractTinkerGraph graph) {
        compactionAttempts.incrementAndGet();
        if (failCompaction)
            throw new UncheckedIOException(new IOException("injected compaction failure"));
        super.compact(graph);
    }

    @Override
    protected void configureCodec(final Configuration config) {
        codec.configureCodec(config);
    }

    @Override
    protected void beginReplay() {
        codec.beginReplay();
    }

    @Override
    protected byte[] encodeCommit(final long txVersion,
                                  final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                  final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException {
        return codec.encodeCommit(txVersion, changedVertices, changedEdges);
    }

    @Override
    protected void decodeFrame(final byte[] record,
                               final Map<Object, DetachedVertex> vertices,
                               final Map<Object, DetachedEdge> edges) throws IOException {
        codec.decodeFrame(record, vertices, edges);
    }

    @Override
    protected void writeSnapshot(final AbstractTinkerGraph graph, final DataOutputStream out) throws IOException {
        if (failSnapshotWrites) {
            out.write(new byte[]{ 0x01, 0x02, 0x03 });
            throw new IOException("injected snapshot write failure");
        }
        codec.writeSnapshot(graph, out);
    }
}
