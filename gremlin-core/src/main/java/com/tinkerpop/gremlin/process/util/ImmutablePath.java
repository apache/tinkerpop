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
public class ImmutablePath implements Path, Serializable, Cloneable {

    private Path previousPath = EmptyPath.instance();
    private Set<String> currentLabels = new HashSet<>();
    private Object currentObject;

    protected ImmutablePath() {

    }

    public static Path make() {
        return EmptyPath.instance();
    }

    public ImmutablePath clone() {
        return this;
    }

    public ImmutablePath(final String currentLabel, final Object currentObject) {
        this(EmptyPath.instance(), currentLabel, currentObject);
    }

    public ImmutablePath(final Set<String> currentLabels, final Object currentObject) {
        this(EmptyPath.instance(), currentLabels, currentObject);
    }

    private ImmutablePath(final Path previousPath, final String currentLabel, final Object currentObject) {
        this.previousPath = previousPath;
        this.currentLabels.add(currentLabel);
        this.currentObject = currentObject;
    }

    private ImmutablePath(final Path previousPath, final Set<String> currentLabels, final Object currentObject) {
        this.previousPath = previousPath;
        this.currentLabels.addAll(currentLabels);
        this.currentObject = currentObject;
    }

    public int size() {
        return this.previousPath.size() + 1;
    }

    public Path extend(final String label, final Object object) {
        return new ImmutablePath(this, label, object);
    }

    public Path extend(final Set<String> labels, final Object object) {
        return new ImmutablePath(this, labels, object);
    }

    public <A> A get(final int index) {
        return (this.size() - 1) == index ? (A) this.currentObject : this.previousPath.get(index);
    }

    public boolean hasLabel(final String label) {
        return this.currentLabels.contains(label) || this.previousPath.hasLabel(label);
    }

    public void addLabel(final String label) {
        if (TraversalHelper.isLabeled(label))
            this.currentLabels.add(label);
    }

    public List<Object> objects() {
        final List<Object> objectPath = new ArrayList<>();
        objectPath.addAll(this.previousPath.objects());
        objectPath.add(this.currentObject);
        return objectPath;
    }

    public List<Set<String>> labels() {
        final List<Set<String>> labelPath = new ArrayList<>();
        labelPath.addAll(this.previousPath.labels());
        labelPath.add(this.currentLabels);
        return labelPath;
    }

    public String toString() {
        return this.objects().toString();
    }
}
