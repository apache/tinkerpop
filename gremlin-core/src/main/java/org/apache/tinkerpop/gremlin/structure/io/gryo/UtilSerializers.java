/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
final class UtilSerializers {

    private UtilSerializers() {
    }

    /**
     * Serializer for {@code List} instances produced by {@code Arrays.asList()}.
     */
    public final static class ArraysAsListSerializer implements SerializerShim<List> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final List list) {
            final List l = new ArrayList(list);
            kryo.writeObject(output, l);
        }

        @Override
        public <I extends InputShim> List read(final KryoShim<I, ?> kryo, final I input, final Class<List> clazz) {
            return kryo.readObject(input, ArrayList.class);
        }
    }

    public final static class ByteBufferSerializer implements SerializerShim<ByteBuffer> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ByteBuffer bb) {
            final byte[] b = bb.array();
            final int arrayOffset = bb.arrayOffset();
            Arrays.copyOfRange(b, arrayOffset + bb.position(), arrayOffset + bb.limit());
            output.writeInt(b.length);
            output.writeBytes(b, 0, b.length);
        }

        @Override
        public <I extends InputShim> ByteBuffer read(final KryoShim<I, ?> kryo, final I input, final Class<ByteBuffer> clazz) {
            final int len = input.readInt();
            final byte[] b = input.readBytes(len);
            final ByteBuffer bb = ByteBuffer.allocate(len);
            bb.put(b);
            return bb;
        }
    }

    public final static class ClassSerializer implements SerializerShim<Class> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Class object) {
            output.writeString(object.getName());
        }

        @Override
        public <I extends InputShim> Class read(final KryoShim<I, ?> kryo, final I input, final Class<Class> clazz) {
            final String name = input.readString();
            try {
                return Class.forName(name);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public final static class ClassArraySerializer implements SerializerShim<Class[]> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Class[] object) {
            output.writeInt(object.length);
            for (final Class clazz : object) {
                output.writeString(clazz.getName());
            }
        }

        @Override
        public <I extends InputShim> Class[] read(final KryoShim<I, ?> kryo, final I input, final Class<Class[]> clazz) {
            final int size = input.readInt();
            final Class[] clazzes = new Class[size];
            for (int i = 0; i < size; i++) {
                try {
                    clazzes[i] = Class.forName(input.readString());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            return clazzes;
        }
    }

    public final static class InetAddressSerializer implements SerializerShim<InetAddress> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final InetAddress addy) {
            final String str = addy.toString().trim();
            final int slash = str.indexOf('/');
            if (slash >= 0) {
                if (slash == 0) {
                    output.writeString(str.substring(1));
                } else {
                    output.writeString(str.substring(0, slash));
                }
            }
        }

        @Override
        public <I extends InputShim> InetAddress read(final KryoShim<I, ?> kryo, final I input, final Class<InetAddress> clazz) {
            try {
                return InetAddress.getByName(input.readString());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public final static class HashSetSupplierSerializer implements SerializerShim<HashSetSupplier> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final HashSetSupplier hashSetSupplier) {
        }

        @Override
        public <I extends InputShim> HashSetSupplier read(final KryoShim<I, ?> kryo, final I input, final Class<HashSetSupplier> clazz) {
            return HashSetSupplier.instance();
        }
    }

    static final class UUIDSerializer implements SerializerShim<UUID> {

        public UUIDSerializer() {
        }

        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final UUID uuid) {
            output.writeLong(uuid.getMostSignificantBits());
            output.writeLong(uuid.getLeastSignificantBits());
        }

        @Override
        public <I extends InputShim> UUID read(final KryoShim<I, ?> kryo, final I input, final Class<UUID> uuidClass) {
            return new UUID(input.readLong(), input.readLong());
        }

        @Override
        public boolean isImmutable() {
            return true;
        }
    }

    static final class URISerializer implements SerializerShim<URI> {

        public URISerializer() {
        }

        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final URI uri) {
            output.writeString(uri.toString());
        }

        @Override
        public <I extends InputShim> URI read(final KryoShim<I, ?> kryo, final I input, final Class<URI> uriClass) {
            return URI.create(input.readString());
        }

        @Override
        public boolean isImmutable() {
            return true;
        }
    }

    static final class PairSerializer implements SerializerShim<Pair> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Pair pair) {
            kryo.writeClassAndObject(output, pair.getValue0());
            kryo.writeClassAndObject(output, pair.getValue1());
        }

        @Override
        public <I extends InputShim> Pair read(final KryoShim<I, ?> kryo, final I input, final Class<Pair> pairClass) {
            return Pair.with(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
        }
    }

    static final class TripletSerializer implements SerializerShim<Triplet> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Triplet triplet) {
            kryo.writeClassAndObject(output, triplet.getValue0());
            kryo.writeClassAndObject(output, triplet.getValue1());
            kryo.writeClassAndObject(output, triplet.getValue2());
        }

        @Override
        public <I extends InputShim> Triplet read(final KryoShim<I, ?> kryo, final I input, final Class<Triplet> tripletClass) {
            return Triplet.with(kryo.readClassAndObject(input), kryo.readClassAndObject(input), kryo.readClassAndObject(input));
        }
    }

    static final class EntrySerializer extends Serializer<Map.Entry> {
        @Override
        public void write(final Kryo kryo, final Output output, final Map.Entry entry) {
            kryo.writeClassAndObject(output, entry.getKey());
            kryo.writeClassAndObject(output, entry.getValue());
        }

        @Override
        public Map.Entry read(final Kryo kryo, final Input input, final Class<Map.Entry> entryClass) {
            return new AbstractMap.SimpleEntry(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
        }
    }

    /**
     * Serializer for {@code List} instances produced by {@code Arrays.asList()}.
     */
    final static class SynchronizedMapSerializer implements SerializerShim<Map> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Map map) {
            final Map m = new LinkedHashMap();
            map.forEach(m::put);
            kryo.writeObject(output, m);
        }

        @Override
        public <I extends InputShim> Map read(final KryoShim<I, ?> kryo, final I input, final Class<Map> clazz) {
            return Collections.synchronizedMap(kryo.readObject(input, LinkedHashMap.class));
        }
    }
}
