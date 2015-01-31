package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends StartStep<S> {

    private final List<S> injections;

    @SafeVarargs
    public InjectStep(final Traversal.Admin traversal, final S... injections) {
        super(traversal);
        this.injections = Arrays.asList(injections);
        this.start = this.injections.iterator();
    }

    @Override
    public InjectStep<S> clone() throws CloneNotSupportedException {
        final InjectStep<S> clone = (InjectStep<S>)super.clone();
        clone.start = this.injections.iterator();
        return clone;
    }
}
