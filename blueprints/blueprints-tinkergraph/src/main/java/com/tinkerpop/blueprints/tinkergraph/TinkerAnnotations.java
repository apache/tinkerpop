package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.util.AnnotationHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerAnnotations implements Annotations, Serializable {

    private final Map<String, Object> annotations = new HashMap<>();

    public <T> Optional<T> get(final String key) {
        return Optional.ofNullable((T) this.annotations.get(key));
    }

    public void set(final String key, final Object value) {
        AnnotationHelper.validateAnnotation(key, value);
        this.annotations.put(key, value);
    }

    public Set<String> getKeys() {
        return this.annotations.keySet();
    }

    public String toString() {
        return this.annotations.toString();
    }
}
