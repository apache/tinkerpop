package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MutablePath implements Path, Serializable, Cloneable {

    protected List<Object> objects = new ArrayList<>();
    protected List<Set<String>> labels = new ArrayList<>();

    protected MutablePath() {

    }

    public static Path make() {
        return new MutablePath();
    }

    public MutablePath clone() {
        final MutablePath clone = new MutablePath();
        for (int i = 0; i < this.objects.size(); i++) {
            clone.objects.add(this.objects.get(i));
            clone.labels.add(new HashSet<>(this.labels.get(i)));
        }
        return clone;
    }


    @Override
    public int size() {
        return this.objects.size();
    }

    @Override
    public Path extend(final String label, final Object object) {
        this.objects.add(object);
        final HashSet<String> temp = new HashSet<>();
        temp.add(label);
        this.labels.add(temp);
        return this;
    }

    @Override
    public Path extend(final Set<String> labels, final Object object) {
        this.objects.add(object);
        this.labels.add(new HashSet<>(labels));
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
        return this.objects;
    }

    @Override
    public List<Set<String>> labels() {
        return this.labels;
    }

    @Override
    public boolean isSimple() {
        return new HashSet<>(this.objects).size() == this.objects.size();
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }
}
