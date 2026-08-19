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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * The GraphBinary {@link TinkerStorage} codec on top of {@link AbstractLogStorage}. The base owns the durable
 * log-structured machinery (file layout, {@code VERSION} marker, CRC framing, replay fold, {@link SyncMode}
 * durability, crash-safe and threshold compaction); this class supplies only how an element is encoded and decoded,
 * using the GraphBinary serializers ({@link GraphBinaryWriter}/{@link GraphBinaryReader}).
 * <p/>
 * Known limitation (write amplification): a commit records each changed element in full — a single property change on
 * a large element rewrites the whole element to the log. Elements are typically small and automatic compaction bounds
 * the resulting log growth, so this is accepted rather than mitigated with per-property deltas, which would complicate
 * the {@link TinkerStorageMutation} contract and the replay fold. The snapshot, by contrast, is streamed one element
 * at a time so compaction never holds a second full copy of the graph in heap.
 */
public final class GraphBinaryStorage extends AbstractLogStorage {

    private static final byte OP_PUT_VERTEX = 1;
    private static final byte OP_DEL_VERTEX = 2;
    private static final byte OP_PUT_EDGE = 3;
    private static final byte OP_DEL_EDGE = 4;

    private final GraphBinaryWriter writer = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE);
    private final GraphBinaryReader reader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE);

    /**
     * Serialize a commit record: txVersion, entry count, then each entry as an op byte followed by either the
     * serialized element (put) or the serialized id (delete).
     */
    @Override
    protected byte[] encodeCommit(final long txVersion,
                                  final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                  final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeLong(txVersion);
        buffer.writeInt(changedVertices.size() + changedEdges.size());
        for (final TinkerStorageMutation<TinkerVertex> m : changedVertices) {
            if (m.isDeleted()) {
                buffer.writeByte(OP_DEL_VERTEX);
                writer.write(m.id(), buffer);
            } else {
                buffer.writeByte(OP_PUT_VERTEX);
                // detach to a stable form independent of the transactional element
                writer.write(DetachedFactory.detach(m.element(), true), buffer);
            }
        }
        for (final TinkerStorageMutation<TinkerEdge> m : changedEdges) {
            if (m.isDeleted()) {
                buffer.writeByte(OP_DEL_EDGE);
                writer.write(m.id(), buffer);
            } else {
                buffer.writeByte(OP_PUT_EDGE);
                writer.write(DetachedFactory.detach(m.element(), true), buffer);
            }
        }
        return buffer.toWrittenArray();
    }

    @Override
    protected void decodeFrame(final byte[] record,
                               final Map<Object, DetachedVertex> vertices,
                               final Map<Object, DetachedEdge> edges) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer(record);
        buffer.readLong(); // txVersion, retained for diagnostics/future use
        final int entryCount = buffer.readInt();
        for (int i = 0; i < entryCount; i++) {
            final byte op = buffer.readByte();
            switch (op) {
                case OP_PUT_VERTEX: {
                    final Vertex v = reader.read(buffer);
                    vertices.put(v.id(), (DetachedVertex) v);
                    break;
                }
                case OP_DEL_VERTEX: {
                    final Object id = reader.read(buffer);
                    vertices.remove(id);
                    break;
                }
                case OP_PUT_EDGE: {
                    final Edge e = reader.read(buffer);
                    edges.put(e.id(), (DetachedEdge) e);
                    break;
                }
                case OP_DEL_EDGE: {
                    final Object id = reader.read(buffer);
                    edges.remove(id);
                    break;
                }
                default:
                    throw new IOException("Unknown storage op code: " + op);
            }
        }
    }

    /**
     * Stream the current committed state as one framed put record per vertex and per edge, so peak memory is bounded
     * to a single element rather than materializing the whole graph as one byte array.
     */
    @Override
    protected void writeSnapshot(final AbstractTinkerGraph graph, final DataOutputStream out) throws IOException {
        final Iterator<Vertex> vertexIterator = graph.vertices();
        while (vertexIterator.hasNext())
            writeElementFrame(out, OP_PUT_VERTEX, vertexIterator.next());
        final Iterator<Edge> edgeIterator = graph.edges();
        while (edgeIterator.hasNext())
            writeElementFrame(out, OP_PUT_EDGE, edgeIterator.next());
    }

    /**
     * Encode a single element as a one-entry put record and write it as a framed record to {@code out}.
     */
    private void writeElementFrame(final DataOutputStream out, final byte op, final Object element) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeLong(0L); // snapshot records have no single tx version
        buffer.writeInt(1);
        buffer.writeByte(op);
        writer.write(DetachedFactory.detach(element, true), buffer);
        writeFrame(out, buffer.toWrittenArray());
    }
}
