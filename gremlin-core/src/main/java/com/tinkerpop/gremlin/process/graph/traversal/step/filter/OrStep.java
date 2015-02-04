package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrStep<S> extends ConjunctionStep<S> implements TraversalParent {

    public OrStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?>... orTraversals) {
        super(traversal, orTraversals);

    }

    public static class OrMarker<S> extends AbstractStep<S, S> {
        public OrMarker(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            throw new IllegalStateException("This step should have been removed via a strategy: " + this.getClass().getCanonicalName());
        }
    }
}
