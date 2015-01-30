package com.tinkerpop.gremlin.process.graph.traversal.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MarkerIdentityStep<S> extends AbstractStep<S, S> {

    public MarkerIdentityStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }
}
