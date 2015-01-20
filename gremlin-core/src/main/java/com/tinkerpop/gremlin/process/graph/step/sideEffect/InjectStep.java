package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends SideEffectStep<S> {

    private final List<S> injections;
    private boolean first = true;

    @SafeVarargs
    public InjectStep(final Traversal traversal, final S... injections) {
        super(traversal);
        this.injections = Arrays.asList(injections);
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first) {
            this.addStarts((Iterator) this.getTraversal().asAdmin().getTraverserGenerator().generateIterator(this.injections.iterator(), this, 1l));
            this.first = false;
        }
        return super.processNextStart();
    }
}
