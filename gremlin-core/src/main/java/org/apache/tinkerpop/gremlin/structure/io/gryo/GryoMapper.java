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

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_PA_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_PA_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DependantMutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoSerializable;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.util.DefaultStreamFactory;
import org.apache.tinkerpop.shaded.kryo.util.MapReferenceResolver;
import org.javatuples.Triplet;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@link Mapper} implementation for Kryo.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoMapper implements Mapper<Kryo> {
    static final byte[] GIO = "gio".getBytes();
    private final List<Triplet<Class, Function<Kryo, Serializer>, Integer>> serializationList;
    private final HeaderWriter headerWriter;
    private final HeaderReader headerReader;
    private final byte[] versionedHeader;

    public static final byte DEFAULT_EXTENDED_VERSION = Byte.MIN_VALUE;

    private GryoMapper(final List<Triplet<Class, Function<Kryo, Serializer>, Integer>> serializationList,
                       final HeaderWriter headerWriter,
                       final HeaderReader headerReader) {
        this.serializationList = serializationList;
        this.headerWriter = headerWriter;
        this.headerReader = headerReader;

        final Output out = new Output(32);
        try {
            this.headerWriter.write(createMapper(), out);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        this.versionedHeader = out.toBytes();
    }

    @Override
    public Kryo createMapper() {
        final Kryo kryo = new Kryo(new GryoClassResolver(), new MapReferenceResolver(), new DefaultStreamFactory());
        kryo.addDefaultSerializer(Map.Entry.class, new EntrySerializer());
        kryo.setRegistrationRequired(true);
        serializationList.forEach(p -> {
            final Function<Kryo, Serializer> serializer = p.getValue1();
            if (null == serializer)
                kryo.register(p.getValue0(), kryo.getDefaultSerializer(p.getValue0()), p.getValue2());
            else
                kryo.register(p.getValue0(), serializer.apply(kryo), p.getValue2());
        });
        return kryo;
    }

    public HeaderWriter getHeaderWriter() {
        return headerWriter;
    }

    public HeaderReader getHeaderReader() {
        return headerReader;
    }

    public List<Class> getRegisteredClasses() {
        return this.serializationList.stream().map(Triplet::getValue0).collect(Collectors.toList());
    }

    /**
     * Gets the header for a Gremlin Kryo file, which is based on the version of Gremlin Kryo that is constructed
     * via the builder classes.
     */
    public byte[] getVersionedHeader() {
        return versionedHeader;
    }

    @FunctionalInterface
    public interface HeaderReader {
        public void read(final Kryo kryo, final Input input) throws IOException;
    }

    @FunctionalInterface
    public interface HeaderWriter {
        public void write(final Kryo kryo, final Output output) throws IOException;
    }

    /**
     * Use a specific version of Gryo.
     */
    public static Builder build(final Version version) {
        return version.getBuilder();
    }

    /**
     * Use the most current version of Gryo.
     */
    public static Builder build() {
        return Version.V_1_0_0.getBuilder();
    }

    public static interface Builder {
        /**
         * Add mapper classes to serializes with gryo using standard serialization.
         */
        public Builder addCustom(final Class... custom);

        /**
         * Add mapper class to serializes with mapper serialization.
         */
        public Builder addCustom(final Class clazz, final Serializer serializer);

        /**
         * Add mapper class to serializes with mapper serialization as returned from a {@link Function}.
         */
        public Builder addCustom(final Class clazz, final Function<Kryo, Serializer> serializer);

        /**
         * If using mapper classes it might be useful to tag the version stamped to the serialization with a mapper
         * value, such that Gryo serialization at 1.0.0 would have a fourth byte for an extended version.  The user
         * supplied fourth byte can then be used to ensure the right deserializer is used to read the data. If this
         * value is not supplied then it is written as {@link Byte#MIN_VALUE}. The value supplied here should be greater
         * than or equal to zero.
         */
        public Builder extendedVersion(final byte extendedVersion);

        /**
         * By default the {@link #extendedVersion(byte)} is checked against what is read from an input source and if
         * those values are equal the version being read is considered "compliant".  To alter this behavior, supply a
         * mapper compliance {@link Predicate} to evaluate the value read from the input source (i.e. first argument)
         * and the value marked in the {@code GryoMapper} instance {i.e. second argument}.  Supplying this function is
         * useful when versions require backward compatibility or other more complex checks.  This function is only used
         * if the {@link #extendedVersion(byte)} is set to something other than its default.
         */
        public Builder compliant(final BiPredicate<Byte, Byte> compliant);

        public GryoMapper create();
    }

    public enum Version {
        V_1_0_0(BuilderV1d0.class);

        private final Class<? extends Builder> builder;

        private Version(final Class<? extends Builder> builder) {
            this.builder = builder;
        }

        Builder getBuilder() {
            try {
                return builder.newInstance();
            } catch (Exception x) {
                throw new RuntimeException("Gryo Builder implementation cannot be instantiated", x);
            }
        }
    }

    public static class BuilderV1d0 implements Builder {

        /**
         * Map with one entry that is used so that it is possible to get the class of LinkedHashMap.Entry.
         */
        private static final LinkedHashMap m = new LinkedHashMap() {{
            put("junk", "dummy");
        }};

        private static final Class LINKED_HASH_MAP_ENTRY_CLASS = m.entrySet().iterator().next().getClass();

        /**
         * Note that the following are pre-registered boolean, Boolean, byte, Byte, char, Character, double, Double,
         * int, Integer, float, Float, long, Long, short, Short, String, void.
         */
        private final List<Triplet<Class, Function<Kryo, Serializer>, Integer>> serializationList = new ArrayList<Triplet<Class, Function<Kryo, Serializer>, Integer>>() {{
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(byte[].class, null, 25));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(char[].class, null, 26));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(short[].class, null, 27));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(int[].class, null, 28));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(long[].class, null, 29));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(float[].class, null, 30));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(double[].class, null, 31));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(String[].class, null, 32));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Object[].class, null, 33));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ArrayList.class, null, 10));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BigInteger.class, null, 34));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BigDecimal.class, null, 35));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Calendar.class, null, 39));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Class.class, null, 41));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collection.class, null, 37));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_LIST.getClass(), null, 51));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_MAP.getClass(), null, 52));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_SET.getClass(), null, 53));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singleton(null).getClass(), null, 54));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singletonList(null).getClass(), null, 24));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singletonMap(null, null).getClass(), null, 23));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Contains.class, null, 49));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Currency.class, null, 40));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Date.class, null, 38));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Direction.class, null, 12));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedEdge.class, null, 21));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedVertexProperty.class, null, 20));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedProperty.class, null, 18));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedVertex.class, null, 19));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedPath.class, null, 60));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(EdgeTerminator.class, null, 14));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(EnumSet.class, null, 46));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashMap.class, null, 11));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashMap.Entry.class, null, 16));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(KryoSerializable.class, null, 36));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LinkedHashMap.class, null, 47));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LinkedHashSet.class, null, 71));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LINKED_HASH_MAP_ENTRY_CLASS, null, 15));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Locale.class, null, 22));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StringBuffer.class, null, 43));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StringBuilder.class, null, 44));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(T.class, null, 48));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TimeZone.class, null, 42));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TreeMap.class, null, 45));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TreeSet.class, null, 50));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(UUID.class, kryo -> new UUIDSerializer(), 17));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(URI.class, kryo -> new URISerializer(), 72));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(VertexTerminator.class, null, 13));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceEdge.class, null, 81));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceVertexProperty.class, null, 82));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceProperty.class, null, 83));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceVertex.class, null, 84));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferencePath.class, null, 85));  // ***LAST ID**

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Edge.class, kryo -> new GraphSerializer.EdgeSerializer(), 65));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Vertex.class, kryo -> new GraphSerializer.VertexSerializer(), 66));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Property.class, kryo -> new GraphSerializer.PropertySerializer(), 67));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(VertexProperty.class, kryo -> new GraphSerializer.VertexPropertySerializer(), 68));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Path.class, kryo -> new GraphSerializer.PathSerializer(), 59));
            // HACK!
            //add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Traverser.Admin.class, gryo -> new GraphSerializer.TraverserSerializer(), 55));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_O_Traverser.class, null, 75));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(O_Traverser.class, null, 76));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_O_P_PA_S_SE_SL_Traverser.class, null, 77));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_O_PA_S_SE_SL_Traverser.class, null, 78));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TraverserSet.class, null, 58));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Tree.class, null, 61));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashSet.class, null, 62));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BulkSet.class, null, 64));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MutableMetrics.class, null, 69));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StandardTraversalMetrics.class, null, 70));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MapMemory.class, null, 73));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MapReduce.NullObject.class, null, 74));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(AtomicLong.class, null, 79));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DependantMutableMetrics.class, null, 80));
        }};

        private static final byte major = 1;
        private static final byte minor = 0;
        private static final byte patchLevel = 0;

        private byte extendedVersion = DEFAULT_EXTENDED_VERSION;
        private BiPredicate<Byte, Byte> compliant = (readExt, serExt) -> readExt.equals(serExt);

        /**
         * Starts numbering classes for Gryo serialization at 65536 to leave room for future usage by TinkerPop.
         */
        private final AtomicInteger currentSerializationId = new AtomicInteger(65536);

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addCustom(final Class... custom) {
            if (custom != null && custom.length > 0)
                serializationList.addAll(Arrays.asList(custom).stream()
                        .map(c -> Triplet.<Class, Function<Kryo, Serializer>, Integer>with(c, null, currentSerializationId.getAndIncrement()))
                        .collect(Collectors.<Triplet<Class, Function<Kryo, Serializer>, Integer>>toList()));
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addCustom(final Class clazz, final Serializer serializer) {
            serializationList.add(Triplet.with(clazz, kryo -> serializer, currentSerializationId.getAndIncrement()));
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addCustom(final Class clazz, final Function<Kryo, Serializer> serializer) {
            serializationList.add(Triplet.with(clazz, serializer, currentSerializationId.getAndIncrement()));
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder extendedVersion(final byte extendedVersion) {
            if (extendedVersion > DEFAULT_EXTENDED_VERSION && extendedVersion < 0)
                throw new IllegalArgumentException("The extendedVersion must be greater than zero");

            this.extendedVersion = extendedVersion;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder compliant(final BiPredicate<Byte, Byte> compliant) {
            if (null == compliant)
                throw new IllegalArgumentException("compliant");

            this.compliant = compliant;
            return this;
        }

        @Override
        public GryoMapper create() {
            return new GryoMapper(serializationList, this::writeHeader, this::readHeader);
        }

        private void writeHeader(final Kryo kryo, final Output output) throws IOException {
            // 32 byte header total
            output.writeBytes(GIO);

            // some space for later
            output.writeBytes(new byte[25]);

            // version x.y.z
            output.writeByte(major);
            output.writeByte(minor);
            output.writeByte(patchLevel);
            output.writeByte(extendedVersion);
        }

        private void readHeader(final Kryo kryo, final Input input) throws IOException {
            if (!Arrays.equals(GIO, input.readBytes(3)))
                throw new IOException("Invalid format - first three bytes of header do not match expected value");

            // skip the next 25 bytes in v1
            input.readBytes(25);

            // final three bytes of header are the version which should be 1.0.0
            final byte[] version = input.readBytes(3);
            final byte extension = input.readByte();

            // direct match on version for now
            if (version[0] != major || version[1] != minor || version[2] != patchLevel)
                throw new IOException(String.format(
                        "The version [%s.%s.%s] in the stream cannot be understood by this reader",
                        version[0], version[1], version[2]));

            if (extendedVersion >= 0 && !compliant.test(extension, extendedVersion))
                throw new IOException(String.format(
                        "The extension [%s] in the input source is not compliant with this configuration of Gryo - [%s]",
                        extension, extendedVersion));
        }
    }
}
