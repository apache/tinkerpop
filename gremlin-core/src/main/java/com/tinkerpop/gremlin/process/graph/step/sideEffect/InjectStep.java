package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends StartStep<S> {

    private final List<S> injections;

    @SafeVarargs
    public InjectStep(final Traversal traversal, final S... injections) {
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
