package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.query.util.DefaultAnnotatedListQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultAnnotatedList<V> implements AnnotatedList<V>, Serializable {

    final List<AnnotatedValue<V>> annotatedValues = new ArrayList<>();

    public AnnotatedValue<V> addValue(final V value, final Object... keyValues) {
        final Annotations annotation = new DefaultAnnotations();
        // TODO: Module 2 check
        for (int i = 0; i < keyValues.length; i = i + 2) {
            annotation.set((String) keyValues[i], keyValues[i + 1]);
        }
        final AnnotatedValue<V> annotatedValue = new DefaultAnnotatedValue<>(value, annotation);
        this.annotatedValues.add(annotatedValue);
        return annotatedValue;
    }

    public boolean isEmpty() {
        return this.annotatedValues.isEmpty();
    }

    public AnnotatedListQuery<V> query() {
        return new DefaultAnnotatedListQuery<V>(this) {
            @Override
            public Iterable<AnnotatedValue<V>> annotatedValues() {
                return (Iterable) StreamFactory.stream(annotatedValues.iterator())
                        .filter(p -> HasContainer.testAllOfAnnotatedValue((AnnotatedValue) p, this.hasContainers))
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

    public class DefaultAnnotatedValue<V> implements AnnotatedValue<V> {

        private final V value;
        private final Annotations annotations;

        public DefaultAnnotatedValue(final V value, final Annotations annotations) {
            this.value = value;
            this.annotations = new DefaultAnnotations();
            annotations.getKeys().forEach(k -> this.annotations.set(k, annotations.get(k).get()));
        }

        public DefaultAnnotatedValue(final V value) {
            this(value, new DefaultAnnotations());
        }

        public V getValue() {
            return this.value;
        }

        public Annotations getAnnotations() {
            return this.annotations;
        }

        public String toString() {
            return "[" + this.value + ":" + this.annotations + "]";
        }
    }
}
