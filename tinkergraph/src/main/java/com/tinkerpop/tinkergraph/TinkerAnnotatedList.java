package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.query.AnnotatedListQuery;
import com.tinkerpop.gremlin.structure.query.util.DefaultAnnotatedListQuery;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotatedList<V> implements AnnotatedList<V>, Serializable {

    final List<AnnotatedValue<V>> annotatedValues = new ArrayList<>();

    public AnnotatedValue<V> addValue(final V value, final Object... annotationKeyValues) {
        final AnnotatedValue<V> annotatedValue = new TinkerAnnotatedValue<V>(value, annotationKeyValues) {
            public void remove() {
                annotatedValues.remove(this);
            }
        };
        this.annotatedValues.add(annotatedValue);
        return annotatedValue;
    }

    public AnnotatedListQuery<V> query() {
        return new DefaultAnnotatedListQuery<V>() {
            @Override
            public Iterable<AnnotatedValue<V>> annotatedValues() {
                return (Iterable) StreamFactory.stream(annotatedValues.iterator())
                        .filter(p -> HasContainer.testAll((AnnotatedValue) p, this.hasContainers))
                        .limit(this.limit)
                        .collect(Collectors.toList());
            }

            @Override
            public Iterable<V> values() {
                return (Iterable) StreamFactory.stream(this.annotatedValues()).map(a -> a.getValue()).collect(Collectors.toList());
            }
        };
    }

    public String toString() {
        return StringFactory.annotatedListString(this);
    }
}

