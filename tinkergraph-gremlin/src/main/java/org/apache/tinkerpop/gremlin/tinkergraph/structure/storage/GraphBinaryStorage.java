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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The GraphBinary {@link TinkerStorage} codec on top of {@link AbstractLogStorage}. The base owns the durable
 * log-structured machinery (file layout, {@code VERSION} marker, CRC framing, replay fold, {@link SyncMode}
 * durability, crash-safe and threshold compaction); this class supplies only how an element is encoded and decoded.
 * <p/>
 * Rather than serialize a whole {@code DetachedVertex}/{@code DetachedEdge} (which repeats every property key and
 * label as a full string on every element and wraps each property in a {@code VertexProperty} envelope), this codec
 * writes element <em>components</em> directly: element ids and property values go through GraphBinary's scalar
 * serializers (a one-byte {@link DataType} tag plus the raw value), while labels, property keys, and meta-property
 * keys are dictionary-encoded to small integer refs. The dictionary is a single dense namespace built by first
 * appearance; new entries are emitted as {@code OP_DICT_APPEND} records inside the same frame, before the entries
 * that reference them, so a torn trailing frame drops a ref and its user atomically.
 * <p/>
 * Vertex-property ids are auto-generated and, by default, not persisted (they are regenerated on load); element and
 * edge ids are always preserved. The snapshot streams one element per frame, so compaction never holds a second full
 * copy of the graph in heap.
 */
public final class GraphBinaryStorage extends AbstractLogStorage {

    private static final byte OP_PUT_VERTEX = 1;
    private static final byte OP_DEL_VERTEX = 2;
    private static final byte OP_PUT_EDGE = 3;
    private static final byte OP_DEL_EDGE = 4;
    private static final byte OP_DICT_APPEND = 5;

    private final TypeSerializerRegistry registry = TypeSerializerRegistry.INSTANCE;
    private final GraphBinaryWriter writer = new GraphBinaryWriter(registry);
    private final GraphBinaryReader reader = new GraphBinaryReader(registry);

    /**
     * Shared string dictionary for labels, property keys, and meta-property keys. Dense ids assigned by first
     * appearance; grows monotonically within a write session and is rebuilt fresh on compaction and on replay.
     */
    private final Map<String, Integer> keyToId = new HashMap<>();
    private final List<String> idToKey = new ArrayList<>();

    @Override
    protected void beginReplay() {
        keyToId.clear();
        idToKey.clear();
    }

    // --------------------------------------------------------------------------------------------- encode

    @Override
    protected byte[] encodeCommit(final long txVersion,
                                  final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                  final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException {
        final ByteBufferBuffer buf = new ByteBufferBuffer();
        // register the strings introduced by this commit (deletes carry only an id, no strings)
        final List<String> appends = new ArrayList<>();
        for (final TinkerStorageMutation<TinkerVertex> m : changedVertices)
            if (!m.isDeleted()) registerVertexStrings(m.element(), appends);
        for (final TinkerStorageMutation<TinkerEdge> m : changedEdges)
            if (!m.isDeleted()) registerEdgeStrings(m.element(), appends);

        writeVarInt(buf, appends.size() + changedVertices.size() + changedEdges.size());
        // dictionary appends first, so every ref below resolves during the fold
        for (final String s : appends) {
            buf.writeByte(OP_DICT_APPEND);
            writeVarInt(buf, keyToId.get(s));
            writeString(buf, s);
        }
        for (final TinkerStorageMutation<TinkerVertex> m : changedVertices) {
            if (m.isDeleted()) {
                buf.writeByte(OP_DEL_VERTEX);
                writeScalar(buf, m.id());
            } else {
                buf.writeByte(OP_PUT_VERTEX);
                writeVertexRecord(buf, m.element());
            }
        }
        for (final TinkerStorageMutation<TinkerEdge> m : changedEdges) {
            if (m.isDeleted()) {
                buf.writeByte(OP_DEL_EDGE);
                writeScalar(buf, m.id());
            } else {
                buf.writeByte(OP_PUT_EDGE);
                writeEdgeRecord(buf, m.element());
            }
        }
        return buf.toWrittenArray();
    }

    @Override
    protected void writeSnapshot(final AbstractTinkerGraph graph, final DataOutputStream out) throws IOException {
        // Preserve the existing dictionary numbering rather than renumbering: the compaction crash window can leave
        // the new snapshot in place with the old log not yet truncated, and that log's refs use the current
        // numbering. Register any not-yet-seen live strings (this only extends the dictionary, never renumbers), then
        // emit the whole dictionary as a self-contained header frame so a snapshot-only replay resolves every ref.
        final List<String> ignored = new ArrayList<>();
        Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext())
            registerVertexStrings(vertices.next(), ignored);
        Iterator<Edge> edges = graph.edges();
        while (edges.hasNext())
            registerEdgeStrings(edges.next(), ignored);

        final ByteBufferBuffer dictBuf = new ByteBufferBuffer();
        writeVarInt(dictBuf, idToKey.size());
        for (int id = 0; id < idToKey.size(); id++) {
            dictBuf.writeByte(OP_DICT_APPEND);
            writeVarInt(dictBuf, id);
            writeString(dictBuf, idToKey.get(id));
        }
        writeFrame(out, dictBuf.toWrittenArray());

        // one element per frame; all keys are already in the dictionary header, so no per-frame appends
        vertices = graph.vertices();
        while (vertices.hasNext())
            writeElementFrame(out, OP_PUT_VERTEX, vertices.next());
        edges = graph.edges();
        while (edges.hasNext())
            writeElementFrame(out, OP_PUT_EDGE, edges.next());
    }

    /**
     * Write one element as its own single-entry put frame. One element is held in memory at a time, so a large graph
     * is never buffered whole.
     */
    private void writeElementFrame(final DataOutputStream out, final byte op, final Object element) throws IOException {
        final ByteBufferBuffer buf = new ByteBufferBuffer();
        writeVarInt(buf, 1);
        buf.writeByte(op);
        if (op == OP_PUT_VERTEX) writeVertexRecord(buf, (Vertex) element);
        else writeEdgeRecord(buf, (Edge) element);
        writeFrame(out, buf.toWrittenArray());
    }

    private void registerVertexStrings(final Vertex v, final List<String> appends) {
        for (final String label : v.labels())
            register(label, appends);
        final Iterator<VertexProperty<Object>> vps = v.properties();
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            register(vp.key(), appends);
            final Iterator<Property<Object>> metas = vp.properties();
            while (metas.hasNext())
                register(metas.next().key(), appends);
        }
    }

    private void registerEdgeStrings(final Edge e, final List<String> appends) {
        register(e.label(), appends);
        final Iterator<Property<Object>> props = e.properties();
        while (props.hasNext())
            register(props.next().key(), appends);
    }

    private void register(final String s, final List<String> appends) {
        if (!keyToId.containsKey(s)) {
            final int id = idToKey.size();
            keyToId.put(s, id);
            idToKey.add(s);
            appends.add(s);
        }
    }

    private void writeVertexRecord(final ByteBufferBuffer buf, final Vertex v) throws IOException {
        writeScalar(buf, v.id());
        final Set<String> labels = v.labels();
        writeVarInt(buf, labels.size());
        for (final String label : labels)
            writeVarInt(buf, keyToId.get(label));

        // group vertex properties by key so multi-properties (list/set) round-trip
        final Map<String, List<VertexProperty<Object>>> groups = new LinkedHashMap<>();
        final Iterator<VertexProperty<Object>> vps = v.properties();
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            groups.computeIfAbsent(vp.key(), k -> new ArrayList<>()).add(vp);
        }
        writeVarInt(buf, groups.size());
        for (final Map.Entry<String, List<VertexProperty<Object>>> group : groups.entrySet()) {
            writeVarInt(buf, keyToId.get(group.getKey()));
            final List<VertexProperty<Object>> values = group.getValue();
            writeVarInt(buf, values.size());
            for (final VertexProperty<Object> vp : values) {
                writeScalar(buf, vp.value());
                final List<Property<Object>> metas = new ArrayList<>();
                vp.properties().forEachRemaining(metas::add);
                writeVarInt(buf, metas.size());
                for (final Property<Object> meta : metas) {
                    writeVarInt(buf, keyToId.get(meta.key()));
                    writeScalar(buf, meta.value());
                }
            }
        }
    }

    private void writeEdgeRecord(final ByteBufferBuffer buf, final Edge e) throws IOException {
        writeScalar(buf, e.id());
        writeVarInt(buf, keyToId.get(e.label()));
        writeScalar(buf, e.outVertex().id());
        writeScalar(buf, e.inVertex().id());
        final List<Property<Object>> props = new ArrayList<>();
        e.properties().forEachRemaining(props::add);
        writeVarInt(buf, props.size());
        for (final Property<Object> p : props) {
            writeVarInt(buf, keyToId.get(p.key()));
            writeScalar(buf, p.value());
        }
    }

    // --------------------------------------------------------------------------------------------- decode

    @Override
    protected void decodeFrame(final byte[] record,
                               final Map<Object, DetachedVertex> vertices,
                               final Map<Object, DetachedEdge> edges) throws IOException {
        final ByteBufferBuffer buf = new ByteBufferBuffer(record);
        final int entryCount = readVarInt(buf);
        for (int i = 0; i < entryCount; i++) {
            final byte op = buf.readByte();
            switch (op) {
                case OP_DICT_APPEND: {
                    final int id = readVarInt(buf);
                    final String s = readString(buf);
                    // idempotent: a snapshot header defines the whole dictionary, and a log surviving the compaction
                    // crash window may re-append entries the snapshot already established. Re-appending an existing
                    // id with the same string is a no-op; a mismatch or a gap is corruption.
                    if (id < idToKey.size()) {
                        if (!idToKey.get(id).equals(s))
                            throw new IOException(String.format("Corrupt storage: dictionary id %d redefined ('%s' vs '%s')", id, idToKey.get(id), s));
                    } else if (id == idToKey.size()) {
                        idToKey.add(s);
                    } else {
                        throw new IOException(String.format("Corrupt storage: dictionary append gap (got %d, expected <= %d)", id, idToKey.size()));
                    }
                    break;
                }
                case OP_PUT_VERTEX: {
                    final DetachedVertex v = readVertexRecord(buf);
                    vertices.put(v.id(), v);
                    break;
                }
                case OP_DEL_VERTEX: {
                    vertices.remove(readScalar(buf));
                    break;
                }
                case OP_PUT_EDGE: {
                    final DetachedEdge e = readEdgeRecord(buf);
                    edges.put(e.id(), e);
                    break;
                }
                case OP_DEL_EDGE: {
                    edges.remove(readScalar(buf));
                    break;
                }
                default:
                    throw new IOException("Unknown storage op code: " + op);
            }
        }
    }

    private DetachedVertex readVertexRecord(final ByteBufferBuffer buf) throws IOException {
        final Object id = readScalar(buf);
        final DetachedVertex.Builder b = DetachedVertex.build().setId(id);
        final int labelCount = readVarInt(buf);
        if (labelCount == 1) {
            b.setLabel(idToKey.get(readVarInt(buf)));
        } else if (labelCount > 1) {
            final Set<String> labels = new LinkedHashSet<>();
            for (int i = 0; i < labelCount; i++)
                labels.add(idToKey.get(readVarInt(buf)));
            b.setLabels(labels);
        }
        final int keyGroupCount = readVarInt(buf);
        for (int g = 0; g < keyGroupCount; g++) {
            final String key = idToKey.get(readVarInt(buf));
            final int valueCount = readVarInt(buf);
            for (int j = 0; j < valueCount; j++) {
                final Object value = readScalar(buf);
                final DetachedVertexProperty.Builder vpb = DetachedVertexProperty.build().setLabel(key).setValue(value);
                final int metaCount = readVarInt(buf);
                for (int m = 0; m < metaCount; m++) {
                    final String metaKey = idToKey.get(readVarInt(buf));
                    final Object metaValue = readScalar(buf);
                    vpb.addProperty(new DetachedProperty<>(metaKey, metaValue));
                }
                b.addProperty(vpb.create());
            }
        }
        return b.create();
    }

    private DetachedEdge readEdgeRecord(final ByteBufferBuffer buf) throws IOException {
        final Object id = readScalar(buf);
        final String label = idToKey.get(readVarInt(buf));
        final Object outVId = readScalar(buf);
        final Object inVId = readScalar(buf);
        final DetachedEdge.Builder b = DetachedEdge.build().setId(id).setLabel(label)
                .setOutV(DetachedVertex.build().setId(outVId).create())
                .setInV(DetachedVertex.build().setId(inVId).create());
        final int propCount = readVarInt(buf);
        for (int i = 0; i < propCount; i++) {
            final String key = idToKey.get(readVarInt(buf));
            final Object value = readScalar(buf);
            b.addProperty(new DetachedProperty<>(key, value));
        }
        return b.create();
    }

    // --------------------------------------------------------------------------------------------- primitives

    /**
     * Write a value as a one-byte {@link DataType} tag followed by the raw value (no value-flag byte). A {@code null}
     * is a single {@link DataType#UNSPECIFIED_NULL} tag.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeScalar(final ByteBufferBuffer buf, final Object value) throws IOException {
        if (value == null) {
            buf.writeByte(DataType.UNSPECIFIED_NULL.getCodeByte());
            return;
        }
        final TypeSerializer serializer = registry.getSerializer(value.getClass());
        buf.writeByte(serializer.getDataType().getCodeByte());
        serializer.writeValue(value, buf, writer, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object readScalar(final ByteBufferBuffer buf) throws IOException {
        final DataType dataType = DataType.get(Byte.toUnsignedInt(buf.readByte()));
        if (dataType == DataType.UNSPECIFIED_NULL)
            return null;
        final TypeSerializer serializer = registry.getSerializer(dataType);
        return serializer.readValue(buf, reader, false);
    }

    private static void writeString(final ByteBufferBuffer buf, final String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeVarInt(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    private static String readString(final ByteBufferBuffer buf) {
        final byte[] bytes = new byte[readVarInt(buf)];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Unsigned LEB128 varint. Counts and dictionary refs are small and non-negative, so they cost one byte in the
     * common case.
     */
    private static void writeVarInt(final ByteBufferBuffer buf, final int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            buf.writeByte((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf.writeByte(v & 0x7F);
    }

    private static int readVarInt(final ByteBufferBuffer buf) {
        int result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf.readByte();
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
