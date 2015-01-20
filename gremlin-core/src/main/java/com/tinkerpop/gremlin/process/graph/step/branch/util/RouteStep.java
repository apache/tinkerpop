package com.tinkerpop.gremlin.process.graph.step.branch.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RouteStep<S> extends AbstractStep<S, S> {

    private final String routeTo;   // TODO: This is just BranchWithGoToPredicate

    public RouteStep(final Traversal traversal, final String routeTo) {
        super(traversal);
        this.routeTo = routeTo;
        this.traverserStepIdSetByChild = true;
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        start.setStepId(this.routeTo);
        return start;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this);
    }
}
