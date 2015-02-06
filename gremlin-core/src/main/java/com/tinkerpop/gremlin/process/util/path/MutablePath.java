package com.tinkerpop.gremlin.process.util.path;

import com.tinkerpop.gremlin.process.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MutablePath implements Path, Serializable {

    final protected List<Object> objects;
    final protected List<Set<String>> labels;

    protected MutablePath() {
        this(0);
    }

    private MutablePath(final int capacity) {
        this.objects = new ArrayList<>(capacity);
        this.labels = new ArrayList<>(capacity);
    }

    public static Path make() {
        return new MutablePath();
    }

    @Override
    public MutablePath clone() throws CloneNotSupportedException {
        final MutablePath clone = new MutablePath(this.objects.size());
        // TODO: Why is this not working Hadoop serialization-wise?... Its cause DetachedPath's clone needs to detach on clone.
        /*final MutablePath clone = (MutablePath) super.clone();
        clone.objects = new ArrayList<>();
        clone.labels = new ArrayList<>();*/
        clone.objects.addAll(this.objects);
        clone.labels.addAll(this.labels);
        return clone;
    }


    @Override
    public int size() {
        return this.objects.size();
    }

    @Override
    public Path extend(final Object object, final String... labels) {
        this.objects.add(object);
        this.labels.add(Stream.of(labels).collect(Collectors.toSet()));
        return this;
    }

    @Override
    public <A> A get(int index) {
        return (A) this.objects.get(index);
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.labels.stream().filter(l -> l.contains(label)).findAny().isPresent();
    }

    @Override
    public void addLabel(final String label) {
        this.labels.get(this.labels.size() - 1).add(label);
    }

    @Override
    public List<Object> objects() {
        return Collections.unmodifiableList(this.objects);
    }

    @Override
    public List<Set<String>> labels() {
        return Collections.unmodifiableList(this.labels);
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }
}
