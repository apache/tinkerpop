package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedList;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedValue;
import org.javatuples.Triplet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GremlinKryo {
    static final byte[] GIO = "gio".getBytes();
    private final List<Triplet<Class, Serializer, Integer>> serializationList;
    private final HeaderWriter headerWriter;
    private final HeaderReader headerReader;

    private GremlinKryo(final List<Triplet<Class, Serializer, Integer>> serializationList,
                        final HeaderWriter headerWriter,
                        final HeaderReader headerReader){
        this.serializationList = serializationList;
        this.headerWriter = headerWriter;
        this.headerReader = headerReader;
    }

    public Kryo createKryo() {
        final Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        serializationList.forEach(p -> {
            final Serializer serializer = Optional.ofNullable(p.getValue1()).orElse(kryo.getDefaultSerializer(p.getValue0()));
            kryo.register(p.getValue0(), serializer, p.getValue2());
        });
        return kryo;
    }

    public HeaderWriter getHeaderWriter() {
        return headerWriter;
    }

    public HeaderReader getHeaderReader() {
        return headerReader;
    }

    @FunctionalInterface
    public interface HeaderReader {
        public void read(final Kryo kryo, final Input input) throws IOException;
    }

    @FunctionalInterface
    public interface HeaderWriter {
        public void write(final Kryo kryo, final Output output) throws IOException;
    }

    public static class UUIDSerializer extends Serializer<UUID> {
        @Override
        public void write(final Kryo kryo, final Output output, final UUID uuid) {
            output.writeLong(uuid.getMostSignificantBits());
            output.writeLong(uuid.getLeastSignificantBits());
        }

        @Override
        public UUID read(final Kryo kryo, final Input input, final Class<UUID> uuidClass) {
            return new UUID(input.readLong(), input.readLong());
        }
    }

    /**
     * Use a specific version of Gremlin Kryo.
     */
    public static Builder create(final Version version) {
        return version.getBuilder();
    }

    /**
     * Use the most current version of Gremlin Kryo.
     */
    public static Builder create() {
        // todo: do this more nicely based on all versions in the enum
        return Version.V_1_0_0.getBuilder();
    }

    public static interface Builder {
        /**
         * Add custom classes to serializes with kryo using standard serialization
         */
        public Builder addCustom(final Class... custom);

        /**
         * If using custom classes it might be useful to tag the version stamped to the serialization with a custom
         * value, such that Kryo serialization at 1.0.0 would have a fourth byte for an extended version.  The
         * user supplied fourth byte can then be used to ensure the right deserializer is used to read the data.
         * If this value is not supplied then it is written as {@link Byte#MIN_VALUE}.
         */
        public Builder extendedVersion(final byte extended);

        public GremlinKryo build();
    }

    public enum Version  {
        V_1_0_0(new BuilderV1d0());
        private final Builder builder;
        private Version(final Builder builder) {
            this.builder = builder;
        }

        Builder getBuilder() {
            return builder;
        }
    }

    public static class BuilderV1d0 implements Builder {
        private final List<Triplet<Class, Serializer, Integer>> serializationList = new ArrayList<Triplet<Class,Serializer, Integer>>() {{
            add(Triplet.<Class, Serializer, Integer>with(ArrayList.class, null, 10));
            add(Triplet.<Class, Serializer, Integer>with(HashMap.class, null, 11));
            add(Triplet.<Class, Serializer, Integer>with(Direction.class, null, 12));
            add(Triplet.<Class, Serializer, Integer>with(VertexTerminator.class, null, 13));
            add(Triplet.<Class, Serializer, Integer>with(EdgeTerminator.class, null, 14));
            add(Triplet.<Class, Serializer, Integer>with(IOAnnotatedList.class, null, 15));
            add(Triplet.<Class, Serializer, Integer>with(IOAnnotatedValue.class, null, 16));
            add(Triplet.<Class, Serializer, Integer>with(UUID.class, new UUIDSerializer(), 17));
        }};

        private static final byte major = 1;
        private static final byte minor = 0;
        private static final byte patchLevel = 0;

        private byte extension = Byte.MIN_VALUE;

        /**
         * Starts numbering classes for Kryo serialization at 8192 to leave room for future usage by TinkerPop.
         */
        private final AtomicInteger currentSerializationId = new AtomicInteger(8192);

        @Override
        public Builder addCustom(final Class... custom) {
            if (custom != null && custom.length > 0)
                serializationList.addAll(Arrays.asList(custom).stream()
                        .map(c->Triplet.<Class, Serializer, Integer>with(c, null, currentSerializationId.getAndIncrement()))
                        .collect(Collectors.<Triplet<Class, Serializer, Integer>>toList()));
            return this;
        }

        @Override
        public Builder extendedVersion(final byte extended) {
            this.extension = extended;
            return this;
        }

        @Override
        public GremlinKryo build() {
            return new GremlinKryo(serializationList, this::writeHeader, this::readHeader);
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
            output.writeByte(extension);
        }

        private void readHeader(final Kryo kryo, final Input input) throws IOException {
            if (!Arrays.equals(GIO, input.readBytes(3)))
                throw new IOException("Invalid format - first three bytes of header do not match expected value");

            // skip the next 25 bytes in v1
            input.readBytes(25);

            // final three bytes of header are the version which should be 1.0.0
            byte[] version = input.readBytes(3);
            byte extension = input.readByte(); // todo:// read compatibility

            // direct match on version for now
            if (version[0] != major || version[1] != minor || version[2] != patchLevel)
                throw new IOException(String.format(
                        "The version [%s.%s.%s] in the stream cannot be understood by this reader",
                        version[0], version[1], version[2]));
        }
    }
}
