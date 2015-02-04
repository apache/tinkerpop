package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndStep<S> extends ConjunctionStep<S> implements TraversalParent {

    public AndStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?>... andTraversals) {
        super(traversal, andTraversals);

    }

    public static final class AndMarker<S> extends AbstractStep<S, S> {
        public AndMarker(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            throw new IllegalStateException("This step should have been removed via a strategy: " + this.getClass().getCanonicalName());
        }
    }
}
