package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalRing;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO: Not connected to GraphTraversal, don't rush it. We can wait post TP3 GA.
public final class UnionStep<S, E> extends AbstractStep<S, E> {

    private final TraversalRing<S, E> traversalRing;

    @SafeVarargs
    public UnionStep(final Traversal traversal, final Traversal<S, E>... branchTraversals) {
        super(traversal);
        this.traversalRing = new TraversalRing<>(branchTraversals);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.traversalRing.size()) {
                final Traversal<S, E> branch = this.traversalRing.next();
                if (branch.hasNext()) return TraversalHelper.getEnd(branch).next();
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.traversalRing.forEach(branch -> branch.addStart(start.makeSibling()));
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }
}
