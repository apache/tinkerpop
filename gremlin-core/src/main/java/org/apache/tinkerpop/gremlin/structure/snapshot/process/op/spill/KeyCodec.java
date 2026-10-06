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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrEdge;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrMetaProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrVertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrVertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Encodes keys and values to bytes and back, so that they can be hashed, compared and spilled.
 * <p>
 * The <em>canonical</em> form of a key is equal for two keys exactly when {@code Object.equals} says they are equal:
 * numbers keep their class ({@code Integer} 1 and {@code Long} 1 differ), float and double use the canonical IEEE bits,
 * sets and maps are sorted by their encoded elements. Vertices and edges are their ordinals. A vertex property is its
 * identifier and an edge or meta property is its key and value, as their {@code equals} says, so these also have a
 * <em>representative</em> form that rebuilds the facade of the first key seen.
 */
final class KeyCodec {

    private static final int NULL = 0;
    private static final int BOOLEAN = 1;
    private static final int BYTE = 2;
    private static final int SHORT = 3;
    private static final int CHAR = 4;
    private static final int INT = 5;
    private static final int LONG = 6;
    private static final int FLOAT = 7;
    private static final int DOUBLE = 8;
    private static final int STRING = 9;
    private static final int UUID_TAG = 10;
    private static final int BIGINT = 11;
    private static final int BIGDEC = 12;
    private static final int LIST = 13;
    private static final int SET = 14;
    private static final int MAP = 15;
    private static final int VERTEX = 16;
    private static final int EDGE = 17;
    private static final int VERTEX_PROPERTY = 18;
    private static final int EDGE_PROPERTY = 19;
    private static final int META_PROPERTY = 20;
    private static final int SERIALIZED = 21;
    private static final int VERTEX_PROPERTY_ID = 22;
    private static final int PROPERTY_KEY_VALUE = 23;

    private final CsrExecutionContext ctx;

    KeyCodec(final CsrExecutionContext ctx) {
        this.ctx = ctx;
    }

    // ---------------------------------------------------------------- keys with a canonical and a representative form

    /**
     * Encodes a key object. The representative bytes are written only when the canonical form cannot rebuild the key.
     */
    void encodeKey(final Object key, final Bytes canon, final Bytes rep) {
        if (key instanceof CsrVertexProperty) {
            canon.putByte(VERTEX_PROPERTY_ID);
            encodeObject(((CsrVertexProperty<?>) key).id(), canon);
            encodeObject(key, rep);
        } else if (key instanceof CsrProperty) {
            final CsrProperty<?> p = (CsrProperty<?>) key;
            canon.putByte(PROPERTY_KEY_VALUE);
            canon.putString(p.key());
            encodeObject(p.value(), canon);
            encodeObject(key, rep);
        } else if (key instanceof CsrMetaProperty) {
            throw new UnsupportedOperationException("A meta-property cannot be a key outside the MP lane");
        } else {
            encodeObject(key, canon);
        }
    }

    /**
     * Encodes entry {@code i} of a batch as a key, without creating a facade when the lane makes that unnecessary.
     */
    void encodeKey(final Batch b, final int i, final Bytes canon, final Bytes rep) {
        switch (b.lane) {
            case V:
                canon.putByte(VERTEX);
                canon.putInt(b.ord[i]);
                break;
            case E:
                canon.putByte(EDGE);
                canon.putInt(b.ord[i]);
                break;
            case VP:
            case EP:
                encodeKey(Materializer.facade(ctx, b, i), canon, rep);
                break;
            case MP: {
                final CsrMetaProperty<?> p = (CsrMetaProperty<?>) Materializer.facade(ctx, b, i);
                canon.putByte(PROPERTY_KEY_VALUE);
                canon.putString(p.key());
                encodeObject(p.value(), canon);
                rep.putByte(META_PROPERTY);
                rep.putInt(b.ord[i]);
                rep.putInt(b.key[i]);
                rep.putLong(b.aux[i]);
                rep.putInt(b.src[i]);
                break;
            }
            default:
                encodeObject(Materializer.value(ctx, b, i), canon);
                break;
        }
    }

    /**
     * Rebuilds the key from its canonical and representative bytes.
     */
    Object decodeKey(final byte[] canon, final byte[] rep) {
        return decodeObject(new ByteSource(rep.length > 0 ? rep : canon));
    }

    // ---------------------------------------------------------------- self-contained values

    void encodeObject(final Object o, final Bytes out) {
        if (o == null) {
            out.putByte(NULL);
        } else if (o instanceof String) {
            out.putByte(STRING);
            out.putString((String) o);
        } else if (o instanceof Long) {
            out.putByte(LONG);
            out.putLong((Long) o);
        } else if (o instanceof Integer) {
            out.putByte(INT);
            out.putInt((Integer) o);
        } else if (o instanceof Boolean) {
            out.putByte(BOOLEAN);
            out.putByte((Boolean) o ? 1 : 0);
        } else if (o instanceof Double) {
            out.putByte(DOUBLE);
            out.putLong(Double.doubleToLongBits((Double) o));
        } else if (o instanceof Float) {
            out.putByte(FLOAT);
            out.putInt(Float.floatToIntBits((Float) o));
        } else if (o instanceof Short) {
            out.putByte(SHORT);
            out.putInt((Short) o);
        } else if (o instanceof Byte) {
            out.putByte(BYTE);
            out.putInt((Byte) o);
        } else if (o instanceof Character) {
            out.putByte(CHAR);
            out.putInt((Character) o);
        } else if (o instanceof UUID) {
            out.putByte(UUID_TAG);
            out.putLong(((UUID) o).getMostSignificantBits());
            out.putLong(((UUID) o).getLeastSignificantBits());
        } else if (o instanceof BigInteger) {
            out.putByte(BIGINT);
            out.putBlock(((BigInteger) o).toByteArray());
        } else if (o instanceof BigDecimal) {
            out.putByte(BIGDEC);
            out.putInt(((BigDecimal) o).scale());
            out.putBlock(((BigDecimal) o).unscaledValue().toByteArray());
        } else if (o instanceof CsrVertex) {
            out.putByte(VERTEX);
            out.putInt(((CsrVertex) o).ordinal());
        } else if (o instanceof CsrEdge) {
            out.putByte(EDGE);
            out.putInt(((CsrEdge) o).ordinal());
        } else if (o instanceof CsrVertexProperty) {
            final CsrVertexProperty<?> p = (CsrVertexProperty<?>) o;
            out.putByte(VERTEX_PROPERTY);
            out.putInt(p.vertexOrdinal());
            out.putInt(p.keyCode());
            out.putLong(p.propertyOrdinal());
        } else if (o instanceof CsrProperty) {
            final CsrProperty<?> p = (CsrProperty<?>) o;
            out.putByte(EDGE_PROPERTY);
            out.putInt(p.edgeOrdinal());
            out.putInt(p.keyCode());
        } else if (o instanceof CsrMetaProperty) {
            throw new UnsupportedOperationException("A meta-property value cannot be spilled outside the MP lane");
        } else if (o instanceof List) {
            final List<?> list = (List<?>) o;
            out.putByte(LIST);
            out.putInt(list.size());
            for (final Object e : list) encodeObject(e, out);
        } else if (o instanceof Set) {
            final Set<?> set = (Set<?>) o;
            final List<byte[]> parts = new ArrayList<>(set.size());
            for (final Object e : set) parts.add(encoded(e));
            parts.sort(Arrays::compareUnsigned);
            out.putByte(SET);
            out.putInt(parts.size());
            for (final byte[] p : parts) out.putBytes(p);
        } else if (o instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) o;
            final List<byte[]> parts = new ArrayList<>(map.size());
            for (final Map.Entry<?, ?> e : map.entrySet()) {
                final Bytes pair = new Bytes();
                encodeObject(e.getKey(), pair);
                encodeObject(e.getValue(), pair);
                parts.add(pair.toArray());
            }
            parts.sort(Arrays::compareUnsigned);
            out.putByte(MAP);
            out.putInt(parts.size());
            for (final byte[] p : parts) out.putBytes(p);
        } else if (o instanceof Serializable) {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream stream = new ObjectOutputStream(bytes)) {
                stream.writeObject(o);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot spill a value of type " + o.getClass().getName(), e);
            }
            out.putByte(SERIALIZED);
            out.putBlock(bytes.toByteArray());
        } else {
            throw new IllegalArgumentException("Cannot spill a value of type " + o.getClass().getName());
        }
    }

    private byte[] encoded(final Object o) {
        final Bytes b = new Bytes();
        encodeObject(o, b);
        return b.toArray();
    }

    Object decodeObject(final ByteSource in) {
        final int tag = in.getByte();
        switch (tag) {
            case NULL:
                return null;
            case BOOLEAN:
                return in.getByte() != 0;
            case BYTE:
                return (byte) in.getInt();
            case SHORT:
                return (short) in.getInt();
            case CHAR:
                return (char) in.getInt();
            case INT:
                return in.getInt();
            case LONG:
                return in.getLong();
            case FLOAT:
                return Float.intBitsToFloat(in.getInt());
            case DOUBLE:
                return Double.longBitsToDouble(in.getLong());
            case STRING:
                return in.getString();
            case UUID_TAG:
                return new UUID(in.getLong(), in.getLong());
            case BIGINT:
                return new BigInteger(in.getBlock());
            case BIGDEC: {
                final int scale = in.getInt();
                return new BigDecimal(new BigInteger(in.getBlock()), scale);
            }
            case LIST: {
                final int n = in.getInt();
                final List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) list.add(decodeObject(in));
                return list;
            }
            case SET: {
                final int n = in.getInt();
                final Set<Object> set = new LinkedHashSet<>();
                for (int i = 0; i < n; i++) set.add(decodeObject(in));
                return set;
            }
            case MAP: {
                final int n = in.getInt();
                final Map<Object, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    final Object k = decodeObject(in);
                    map.put(k, decodeObject(in));
                }
                return map;
            }
            case VERTEX:
                return ctx.graph().vertexAt(in.getInt());
            case EDGE:
                return ctx.graph().edgeAt(in.getInt());
            case VERTEX_PROPERTY: {
                final int vertex = in.getInt();
                final int keyCode = in.getInt();
                return ctx.graph().vertexPropertyAt(vertex, keyCode, in.getLong());
            }
            case EDGE_PROPERTY: {
                final int edge = in.getInt();
                return ctx.graph().edgePropertyAt(edge, in.getInt());
            }
            case META_PROPERTY: {
                final int vertex = in.getInt();
                final int keyCode = in.getInt();
                final long ordinal = in.getLong();
                return ctx.graph().metaPropertyAt(vertex, keyCode, ordinal, in.getInt());
            }
            case SERIALIZED:
                try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(in.getBlock()))) {
                    return stream.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new IllegalStateException("Cannot read a spilled value", e);
                }
            default:
                throw new IllegalStateException("Unknown spilled value tag " + tag);
        }
    }
}
