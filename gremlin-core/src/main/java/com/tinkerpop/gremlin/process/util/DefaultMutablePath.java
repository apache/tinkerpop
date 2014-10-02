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
public class DefaultMutablePath implements Path, Serializable, Cloneable {

    protected List<Object> objects = new ArrayList<>();
    protected List<Set<String>> labels = new ArrayList<>();

    public DefaultMutablePath() {

    }

    public DefaultMutablePath clone() {
        final DefaultMutablePath clone = new DefaultMutablePath();
        this.forEach((labels, object) -> {
            clone.objects.add(object);
            clone.labels.add(new HashSet<>(labels));
        });
        return clone;
    }


    @Override
    public int size() {
        return this.objects.size();
    }

    @Override
    public Path extend(final String label, final Object object) {
        this.objects.add(object);
        this.labels.add(new HashSet<String>() {{
            add(label);
        }});
        return this;
    }

    @Override
    public Path extend(final Set<String> labels, final Object object) {
        this.objects.add(object);
        this.labels.add(new HashSet<>(labels));
        return this;
    }

    @Override
    public <A> A get(final String label) {
        for (int i = 0; i < this.objects.size(); i++) {
            if (this.labels.get(i).contains(label)) {
                return (A) this.objects.get(i);
            }
        }
        throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
    }

    @Override
    public <A> A get(int index) {
        return (A) this.objects.get(index);
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.labels.stream().filter(l -> l.contains(label)).findFirst().isPresent();
    }

    @Override
    public void addLabel(final String label) {
        this.labels.get(this.labels.size() - 1).add(label);
    }

    @Override
    public List<Object> getObjects() {
        return this.objects;
    }

    @Override
    public List<Set<String>> getLabels() {
        return this.labels;
    }

    @Override
    public boolean isSimple() {
        return new HashSet<>(this.objects).size() == this.objects.size();
    }
}
