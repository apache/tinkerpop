package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.query.util.DefaultAnnotatedListQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultAnnotatedList<V> implements AnnotatedList<V>, Serializable {

    final List<AnnotatedValue<V>> values = new ArrayList<>();

    public DefaultAnnotatedList(final List<V> initialValues) {
        initialValues.forEach(v -> this.values.add(new DefaultAnnotatedValue<>(v)));
    }

    public void add(final V value, final Object... keyValues) {
        final Annotations annotation = new DefaultAnnotations();
        // TODO: Module 2 check
        for (int i = 0; i < keyValues.length; i = i + 2) {
            annotation.put((String) keyValues[i], Optional.of(keyValues[i + 1]));
        }
        this.values.add(new DefaultAnnotatedValue<>(value, annotation));
    }

    public String toString() {
        final List<V> list = new ArrayList<>();
        this.values.forEach(a -> list.add(a.getValue()));
        return list.toString();
    }

    public AnnotatedListQuery query() {
        return new DefaultAnnotatedListQuery<V>(this) {
            @Override
            public Iterable<AnnotatedValue<V>> values() {
                return (Iterable) StreamFactory.stream(values.iterator())
                        .filter(p -> HasContainer.testAllAnnotations((AnnotatedValue) p, this.hasContainers))
                        .collect(Collectors.toList());
            }
        };
    }

    public class DefaultAnnotations extends HashMap<String, Optional<Object>> implements Annotations {

        @Override
        public Optional<Object> get(final Object key) {
            if (this.containsKey(key))
                return super.get(key);
            else
                return Optional.empty();
        }
    }

    public class DefaultAnnotatedValue<V> implements AnnotatedValue<V> {

        private final V value;
        private final Annotations annotations;

        public DefaultAnnotatedValue(final V value, final Annotations annotations) {
            this.value = value;
            this.annotations = annotations;
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
