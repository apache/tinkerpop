package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Serializable form of an {@link AnnotatedValue} for IO purposes.  Note that this implementation is used when a
 * {@link AnnotatedValue} is serialized on its own without an {@link IoAnnotatedList}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoAnnotatedValue<V> {

    public V value;
    public Map<String, Object> annotations = new HashMap<>();

    /**
     * Converts a set of memory in a {@link Map} to an array of key-value objects.  This is the format expected
     * when doing a {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)},
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)}
     * {@link com.tinkerpop.gremlin.structure.Element#setProperties(Object...)}.
     */
    public Object[] toAnnotationsArray() {
        return this.annotations.entrySet().stream()
                .flatMap(kv -> Stream.of(kv.getKey(), kv.getValue()))
                .collect(Collectors.toList()).toArray();
    }

    public static <V> IoAnnotatedValue from(final AnnotatedValue<V> av) {
        final IoAnnotatedValue<V> kav = new IoAnnotatedValue<>();
        kav.value = av.getValue();
        kav.annotations = av.getAnnotationKeys().stream()
                .map(key -> Pair.<String, Optional>with(key, av.getAnnotation(key)))
                .filter(kv -> kv.getValue1().isPresent())
                .map(kv -> Pair.<String, Object>with(kv.getValue0(), kv.getValue1().get()))
                .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));

        return kav;
    }
}
