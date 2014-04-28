package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.AnnotatedValueHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedAnnotatedValue<V> implements AnnotatedValue<V> {

    private final V value;
    private final Map<String, Object> annotations;

    public CachedAnnotatedValue(final V value, final Map<String,Object> annotationKeyValues) {
        this.value = value;
        this.annotations = annotationKeyValues;
    }

    public V getValue() {
        return this.value;
    }


    public <T> Optional<T> getAnnotation(final String key) {
        return Optional.ofNullable((T) this.annotations.get(key));
    }

    public void setAnnotation(final String key, final Object value) {
        throw new UnsupportedOperationException("Cached annotations are readonly: " + this.toString());
    }

    public Set<String> getAnnotationKeys() {
        return this.annotations.keySet();
    }

    public void removeAnnotation(final String key) {
        throw new UnsupportedOperationException("Cached annotations are readonly: " + this.toString());
    }

    public void remove() {

    }

    public String toString() {
        return StringFactory.annotatedValueString(this);
    }
}