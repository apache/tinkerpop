package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
    public Path extend(final Object object, final String... labels) {
        this.currentObject = object;
        if (labels.length > 0)
            Stream.of(labels).forEach(label -> this.map.put(label, object));
        return this;
    }

    @Override
    public void addLabel(final String label) {
        this.map.put(label, this.currentObject);
    }

    @Override
    public List<Object> objects() {
        return Collections.unmodifiableList(new ArrayList<>(this.map.values()));
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labels = new ArrayList<>();
        this.map.forEach((k, v) -> labels.add(Collections.singleton(k)));
        return Collections.unmodifiableList(labels);
    }


    public <A> A get(final String label) throws IllegalArgumentException {
        final Object object = this.map.get(label);
        if (null == object)
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        return (A) object;
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.map.containsKey(label);
    }

    @Override
    public Path clone() {
        return this;
    }

    @Override
    public int size() {
        return this.map.size();
    }
}
