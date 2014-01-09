package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.query.util.DefaultAnnotatedListQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultAnnotatedList<V> implements AnnotatedList<V>, Serializable {

    final List<Pair<V, Annotations>> values = new ArrayList<>();

    public DefaultAnnotatedList(final List<V> initialValues) {
        initialValues.forEach(v -> this.values.add(new Pair<V, Annotations>(v, new DefaultAnnotations())));
    }

    public Pair<V, Annotations> get(int index) {
        return this.values.get(index);
    }

    public void add(final V value) {
        this.values.add(new Pair<V, Annotations>(value, new DefaultAnnotations()));
    }

    public void add(final V value, final Object... annotationKeyValues) {
        final Annotations annotation = new DefaultAnnotations();
        // TODO: Module 2 check
        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            annotation.put((String) annotationKeyValues[i], Optional.of(annotationKeyValues[i + 1]));
        }
        this.values.add(new Pair<>(value, annotation));
    }

    public String toString() {
        final List<V> list = new ArrayList<>();
        this.values.forEach(p -> list.add(p.getA()));
        return list.toString();
    }

    public Iterator<Pair<V, Annotations>> iterator() {
        return this.values.iterator();
    }

    public Iterator<V> valueIterator() {
        final Iterator<Pair<V, Annotations>> itty = values.iterator();
        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public V next() {
                return itty.next().getA();
            }
        };
    }

    public AnnotatedListQuery query() {
        return new DefaultAnnotatedListQuery(this) {
            @Override
            public <V> Iterable<Pair<V, Annotations>> values() {
                return (Iterable) StreamFactory.stream(this.annotatedList.iterator())
                        .filter(p -> HasContainer.testAllAnnotations((Pair) p, this.hasContainers))
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
}
