package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterStep<S> extends AbstractStep<S, S> {

    public SPredicate<Traverser<S>> predicate = null;
    public SBiPredicate<Traverser<S>, Traversal.SideEffects> biPredicate = null;

    public FilterStep(final Traversal traversal) {
        super(traversal);
    }

    public void setPredicate(final SPredicate<Traverser<S>> predicate) {
        this.predicate = predicate;
    }

    public void setBiPredicate(final SBiPredicate<Traverser<S>, Traversal.SideEffects> biPredicate) {
        this.biPredicate = biPredicate;
    }

    protected Traverser<S> processNextStart() {
        while (true) {
            final Traverser<S> traverser = this.starts.next();
            if (null == this.biPredicate) {
                if (this.predicate.test(traverser))
                    return traverser;
            } else {
                if (this.biPredicate.test(traverser, this.traversal.sideEffects()))
                    return traverser;
            }
        }
    }
}
