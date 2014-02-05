package com.tinkerpop.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.util.AnnotatedValueHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotatedValue<V> implements AnnotatedValue<V>, Serializable {

    private final V value;
    private final Map<String, Object> annotations = new HashMap<>();

    public TinkerAnnotatedValue(final V value, final Object... annotationKeyValues) {
        AnnotatedValueHelper.validateAnnotatedValue(value);
        AnnotatedValueHelper.legalAnnotationKeyValueArray(annotationKeyValues);
        this.value = value;
        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            this.annotations.put((String) annotationKeyValues[i], annotationKeyValues[i + 1]);
        }
    }

    public V getValue() {
        return this.value;
    }


    public <T> Optional<T> getAnnotation(final String key) {
        return Optional.ofNullable((T) this.annotations.get(key));
    }

    public void setAnnotation(final String key, final Object value) {
        AnnotatedValueHelper.validateAnnotation(key, value);
        this.annotations.put(key, value);
    }

    public Set<String> getAnnotationKeys() {
        return this.annotations.keySet();
    }

    public void removeAnnotation(final String key) {
        this.annotations.remove(key);
    }

    public void remove() {

    }

    public String toString() {
        return StringFactory.annotatedValueString(this);
    }

}