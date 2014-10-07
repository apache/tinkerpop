package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraverserIterator;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends SideEffectStep<S> implements TraverserSource {

    private final List<S> injections;

    @SafeVarargs
    public InjectStep(final Traversal traversal, final S... injections) {
        super(traversal);
        this.injections = Arrays.asList(injections);
    }

    @Override
    public void generateTraverserIterator(final boolean trackPaths) {
        this.addStarts(new TraverserIterator<>(this, trackPaths, this.injections.iterator()));
    }

    @Override
    public void clear() {
        this.starts.clear();
    }
}
