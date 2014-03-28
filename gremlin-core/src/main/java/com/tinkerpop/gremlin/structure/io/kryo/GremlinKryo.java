package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Direction;
import org.javatuples.Triplet;

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
    private final List<Triplet<Class, Serializer, Integer>> serializationList = new ArrayList<Triplet<Class,Serializer, Integer>>() {{
        add(Triplet.<Class, Serializer, Integer>with(ArrayList.class, null, 10));
        add(Triplet.<Class, Serializer, Integer>with(HashMap.class, null, 11));
        add(Triplet.<Class, Serializer, Integer>with(Direction.class, null, 12));
        add(Triplet.<Class, Serializer, Integer>with(VertexTerminator.class, null, 13));
        add(Triplet.<Class, Serializer, Integer>with(EdgeTerminator.class, null, 14));
        add(Triplet.<Class, Serializer, Integer>with(KryoAnnotatedList.class, null, 15));
        add(Triplet.<Class, Serializer, Integer>with(KryoAnnotatedValue.class, null, 16));
        add(Triplet.<Class, Serializer, Integer>with(UUID.class, new UUIDSerializer(), 17));
    }};

    /**
     * Starts numbering classes for Kryo serialization at 8192 to leave room for future usage by TinkerPop.
     */
    private final AtomicInteger currentSerializationId = new AtomicInteger(8192);

    public synchronized void addCustom(final Class... custom) {
        if (custom.length > 0)
            serializationList.addAll(Arrays.asList(custom).stream()
                    .map(c->Triplet.<Class, Serializer, Integer>with(c, null, currentSerializationId.getAndIncrement()))
                    .collect(Collectors.<Triplet<Class, Serializer, Integer>>toList()));
    }

    public Kryo create() {
        final Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        serializationList.forEach(p -> {
            final Serializer serializer = Optional.ofNullable(p.getValue1()).orElse(kryo.getDefaultSerializer(p.getValue0()));
            kryo.register(p.getValue0(), serializer, p.getValue2());
        });
        return kryo;
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
}
