package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparsePath implements Path {

    private final Map<String, Object> map = new HashMap<>();
    private Object currentObject = null;

    protected SparsePath() {

    }

    public static SparsePath make() {
        return new SparsePath();
    }

    @Override
    public Path extend(final String label, final Object object) {
        this.currentObject = object;
        this.map.put(label, object);
        return this;
    }

    @Override
    public Path extend(final Set<String> labels, final Object object) {
        this.currentObject = object;
        for (final String label : labels) {
            this.map.put(label, object);
        }
        return this;
    }

    @Override
    public void addLabel(final String label) {
        this.map.put(label, this.currentObject);
    }

    @Override
    public List<Object> objects() {
        return new ArrayList<>(this.map.values());
    }

    @Override
    public List<Set<String>> labels() {
        return Collections.singletonList(this.map.keySet());
    }


    public <A> A get(final String label) throws IllegalArgumentException {
        final Object object = this.map.get(label);
        if (null == object)
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        return (A) object;
    }

    @Override
    public Path clone() {
        return this;
    }
}
