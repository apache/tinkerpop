package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import org.javatuples.Pair;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Serializable form of an {@link AnnotatedList} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoAnnotatedList<V> {
    public List<IoListValue<V>> annotatedValueList;

    public static <V> IoAnnotatedList<V> from(final AnnotatedList<V> annotatedList) {
        final IoAnnotatedList<V> kal = new IoAnnotatedList<>();
        kal.annotatedValueList = annotatedList.annotatedValues().toList().stream()
                .map(IoListValue::from).collect(Collectors.toList());
        return kal;
    }

    /**
     * An extension to the {@link IoAnnotatedValue} that is specifically used when serializing a
     * {@link IoAnnotatedValue} as part of a {@link AnnotatedList}.
     */
    public static class IoListValue<V> extends IoAnnotatedValue<V> {
        public static <V> IoListValue<V> from(final AnnotatedValue<V> av) {
            final IoListValue<V> kav = new IoListValue<>();
            kav.value =av.getValue();
            kav.annotations = av.getAnnotationKeys().stream()
                    .map(key -> Pair.<String, Optional>with(key, av.getAnnotation(key)))
                    .filter(kv -> kv.getValue1().isPresent())
                    .map(kv -> Pair.<String, Object>with(kv.getValue0(), kv.getValue1().get()))
                    .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));

            return kav;
        }
    }
}
