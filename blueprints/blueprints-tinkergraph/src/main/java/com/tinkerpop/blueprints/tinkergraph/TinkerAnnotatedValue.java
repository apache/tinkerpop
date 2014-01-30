package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.util.AnnotationHelper;
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

    public TinkerAnnotatedValue(final V value, final Object... keyValues) {
        AnnotationHelper.validatedAnnotatedValue(value);
        AnnotationHelper.legalKeyValues(keyValues);
        this.value = value;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            this.annotations.put((String) keyValues[i], keyValues[i + 1]);
        }
    }

    public V getValue() {
        return this.value;
    }


    public <T> Optional<T> getAnnotation(final String key) {
        return Optional.ofNullable((T) this.annotations.get(key));
    }

    public void setAnnotation(final String key, final Object value) {
        AnnotationHelper.validateAnnotation(key, value);
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