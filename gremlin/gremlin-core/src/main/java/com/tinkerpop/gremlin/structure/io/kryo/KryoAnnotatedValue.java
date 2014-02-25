package com.tinkerpop.gremlin.structure.io.kryo;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class KryoAnnotatedValue<V> {

    private V value;
    private Map<String, Object> annotations = new HashMap<>();

    public V getValue() {
        return value;
    }

    public void setValue(final V value) {
        this.value = value;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(final Map<String, Object> annotations) {
        this.annotations = annotations;
    }

    public Object[] getAnnotationsArray() {
        return this.annotations.entrySet().stream()
                .flatMap(kv -> Stream.of(kv.getKey(), kv.getValue()))
                .collect(Collectors.toList()).toArray();
    }

    public static <V> KryoAnnotatedValue from(final AnnotatedValue<V> av) {
        final KryoAnnotatedValue<V> kav = new KryoAnnotatedValue<>();
        kav.setValue(av.getValue());

        final Map<String, Object> map = av.getAnnotationKeys().stream()
                .map(key -> Pair.<String, Optional>with(key, av.getAnnotation(key)))
                .filter(kv -> kv.getValue1().isPresent())
                .map(kv -> Pair.<String, Object>with(kv.getValue0(), kv.getValue1().get()))
                .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
        kav.setAnnotations(map);

        return kav;
    }
}
