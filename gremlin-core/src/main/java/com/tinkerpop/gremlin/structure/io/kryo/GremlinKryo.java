package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.tinkerpop.gremlin.structure.Direction;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GremlinKryo {
    private final List<Pair<Class, Integer>> serializationList = new ArrayList<Pair<Class,Integer>>() {{
        add(Pair.<Class, Integer>with(ArrayList.class, 10));
        add(Pair.<Class, Integer>with(HashMap.class, 11));
        add(Pair.<Class, Integer>with(Direction.class, 12));
        add(Pair.<Class, Integer>with(VertexTerminator.class, 13));
        add(Pair.<Class, Integer>with(EdgeTerminator.class, 14));
        add(Pair.<Class, Integer>with(KryoAnnotatedList.class, 15));
        add(Pair.<Class, Integer>with(KryoAnnotatedValue.class, 16));
    }};

    /**
     * Starts numbering classes for Kryo serialization at 8192 to leave room for future usage by TinkerPop.
     */
    private final AtomicInteger currentSerializationId = new AtomicInteger(8192);

    public synchronized void addCustom(final Class... custom) {
        if (custom.length > 0)
            serializationList.addAll(Arrays.asList(custom).stream()
                    .map(c->Pair.<Class, Integer>with(c, currentSerializationId.getAndIncrement()))
                    .collect(Collectors.<Pair<Class, Integer>>toList()));
    }

    public Kryo create() {
        final Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        serializationList.forEach(p-> kryo.register(p.getValue0(), p.getValue1()));
        return kryo;
    }
}
