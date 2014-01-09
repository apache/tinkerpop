package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
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

    public DefaultAnnotatedList(final V... values) {
        for (V value : values) {
            this.annotatedValues.add(new DefaultAnnotatedValue<>(value));
        }
    }

    public DefaultAnnotatedList(final AnnotatedValue<V>... annotatedValues) {
        for (AnnotatedValue<V> annotatedValue : annotatedValues) {
            this.annotatedValues.add(new DefaultAnnotatedValue<>(annotatedValue.getValue(), annotatedValue.getAnnotations()));
        }
    }

    public void add(final V value, final Object... keyValues) {
        final Annotations annotation = new DefaultAnnotations();
        // TODO: Module 2 check
        for (int i = 0; i < keyValues.length; i = i + 2) {
            annotation.set((String) keyValues[i], keyValues[i + 1]);
        }
        this.annotatedValues.add(new DefaultAnnotatedValue<>(value, annotation));
    }

    public String toString() {
        final List<String> list = new ArrayList<>();
        this.annotatedValues.forEach(a -> list.add(a.getValue().toString()));
        return list.toString();
    }

    public AnnotatedListQuery<V> query() {
        return new DefaultAnnotatedListQuery<V>(this) {
            @Override
            public Iterable<AnnotatedValue<V>> values() {
                return (Iterable) StreamFactory.stream(annotatedValues.iterator())
                        .filter(p -> HasContainer.testAllAnnotations((AnnotatedValue) p, this.hasContainers))
                        .collect(Collectors.toList());
            }
        };
    }

    public class DefaultAnnotations implements Annotations {

        private final Map<String, Object> annotations = new HashMap<>();

        public <T> Optional<T> get(final String key) {
            return Optional.ofNullable((T) this.annotations.get(key));
        }

        public void set(final String key, final Object value) {
            this.annotations.put(key, value);
        }

        public Set<String> getKeys() {
            return this.annotations.keySet();
        }

        public String toString() {
            return this.annotations.toString();
        }
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
