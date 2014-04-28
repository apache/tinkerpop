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
public class IOAnnotatedList<V> {
    private List<IOListValue> annotatedValueList;

    public List<IOListValue> getAnnotatedValueList() {
        return annotatedValueList;
    }

    public void setAnnotatedValueList(final List<IOListValue> annotatedValueList) {
        this.annotatedValueList = annotatedValueList;
    }

    public static <V> IOAnnotatedList<V> from(final AnnotatedList<V> annotatedList) {
        final IOAnnotatedList<V> kal = new IOAnnotatedList<>();
        kal.setAnnotatedValueList(annotatedList.annotatedValues().toList().stream()
                .map(IOListValue::from).collect(Collectors.toList()));
        return kal;
    }

    public static class IOListValue<V> extends IOAnnotatedValue<V> {
        public static <V> IOListValue from(final AnnotatedValue<V> av) {
            final IOListValue<V> kav = new IOListValue<>();
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
