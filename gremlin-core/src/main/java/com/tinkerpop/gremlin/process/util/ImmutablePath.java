package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ImmutablePath implements Path, Serializable, Cloneable {

    private Path previousPath = HeadPath.instance();
    private Object currentObject;
    private Set<String> currentLabels = new HashSet<>();

    protected ImmutablePath() {

    }

    public static Path make() {
        return HeadPath.instance();
    }

    public ImmutablePath clone() throws CloneNotSupportedException {
        return this;
    }

    public ImmutablePath(final Object currentObject, final String... currentLabels) {
        this(HeadPath.instance(), currentObject, currentLabels);
    }

    private ImmutablePath(final Path previousPath, final Object currentObject, final String... currentLabels) {
        this.previousPath = previousPath;
        this.currentObject = currentObject;
        if (currentLabels.length > 0)
            Stream.of(currentLabels).forEach(this.currentLabels::add);
    }

    @Override
    public int size() {
        return this.previousPath.size() + 1;
    }

    @Override
    public Path extend(final Object object, final String... labels) {
        return new ImmutablePath(this, object, labels);
    }

    @Override
    public <A> A get(final int index) {
        return (this.size() - 1) == index ? (A) this.currentObject : this.previousPath.get(index);
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.currentLabels.contains(label) || this.previousPath.hasLabel(label);
    }

    @Override
    public void addLabel(final String label) {
        this.currentLabels.add(label);
    }

    @Override
    public List<Object> objects() {
        final List<Object> objectPath = new ArrayList<>();
        objectPath.addAll(this.previousPath.objects());
        objectPath.add(this.currentObject);
        return Collections.unmodifiableList(objectPath);
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labelPath = new ArrayList<>();
        labelPath.addAll(this.previousPath.labels());
        labelPath.add(this.currentLabels);
        return Collections.unmodifiableList(labelPath);
    }

    @Override
    public String toString() {
        return this.objects().toString();
    }

    private static class HeadPath implements Path {
        private static final HeadPath INSTANCE = new HeadPath();

        private HeadPath() {

        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Path extend(final Object object, final String... labels) {
            return new ImmutablePath(object, labels);
        }

        @Override
        public <A> A get(final String label) {
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        }

        @Override
        public <A> A get(final int index) {
            return (A) Collections.emptyList().get(index);
        }

        @Override
        public boolean hasLabel(final String label) {
            return false;
        }

        @Override
        public void addLabel(final String label) {
            throw new UnsupportedOperationException("A head path can not have labels added to it");
        }

        @Override
        public List<Object> objects() {
            return Collections.emptyList();
        }

        @Override
        public List<Set<String>> labels() {
            return Collections.emptyList();
        }

        @Override
        public boolean isSimple() {
            return true;
        }

        @Override
        public HeadPath clone() {
            return this;
        }

        public static Path instance() {
            return INSTANCE;
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof HeadPath;
        }

        @Override
        public String toString() {
            return Collections.emptyList().toString();
        }
    }
}
