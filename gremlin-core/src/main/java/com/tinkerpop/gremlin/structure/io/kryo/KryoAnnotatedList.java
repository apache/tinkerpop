package com.tinkerpop.gremlin.structure.io.kryo;

import com.tinkerpop.gremlin.structure.AnnotatedList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Kryo serializable form of an {@link AnnotatedList}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class KryoAnnotatedList<V> {
    private List<KryoAnnotatedValue> annotatedValueList;

    public List<KryoAnnotatedValue> getAnnotatedValueList() {
        return annotatedValueList;
    }

    public void setAnnotatedValueList(final List<KryoAnnotatedValue> annotatedValueList) {
        this.annotatedValueList = annotatedValueList;
    }

    public static <V> KryoAnnotatedList<V> from(final AnnotatedList<V> annotatedList) {
        final KryoAnnotatedList<V> kal = new KryoAnnotatedList<>();
        kal.setAnnotatedValueList(annotatedList.annotatedValues().toList().stream()
                .map(KryoAnnotatedValue::from).collect(Collectors.toList()));
        return kal;
    }
}
