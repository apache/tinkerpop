package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyPath implements Path, Serializable {

    private static final EmptyPath INSTANCE = new EmptyPath();

    private EmptyPath() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Path extend(final String label, final Object object) {
        return new ImmutablePath(this, label, object);
    }

    @Override
    public Path extend(final Set<String> labels, final Object object) {
        return new ImmutablePath(this, labels, object);
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
        throw new IllegalStateException("Empty path can not have labels added to it");
    }

    @Override
    public List<Object> getObjects() {
        return Collections.emptyList();
    }

    @Override
    public List<Set<String>> getLabels() {
        return Collections.emptyList();
    }

    @Override
    public boolean isSimple() {
        return true;
    }

    @Override
    public EmptyPath clone() {
        return this;
    }

    public static Path instance() {
        return INSTANCE;
    }


}
