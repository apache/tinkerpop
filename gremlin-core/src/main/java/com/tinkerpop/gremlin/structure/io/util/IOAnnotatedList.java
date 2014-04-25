package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.AnnotatedList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Serializable form of an {@link AnnotatedList} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IOAnnotatedList<V> {
    private List<IOAnnotatedValue> annotatedValueList;

    public List<IOAnnotatedValue> getAnnotatedValueList() {
        return annotatedValueList;
    }

    public void setAnnotatedValueList(final List<IOAnnotatedValue> annotatedValueList) {
        this.annotatedValueList = annotatedValueList;
    }

    public static <V> IOAnnotatedList<V> from(final AnnotatedList<V> annotatedList) {
        final IOAnnotatedList<V> kal = new IOAnnotatedList<>();
        kal.setAnnotatedValueList(annotatedList.annotatedValues().toList().stream()
                .map(IOAnnotatedValue::from).collect(Collectors.toList()));
        return kal;
    }
}
