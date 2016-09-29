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

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
final class UtilSerializers {

    private UtilSerializers() {}

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

        public UUIDSerializer() { }

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

        public URISerializer() { }

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
}
