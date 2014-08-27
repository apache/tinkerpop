package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalRing;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO: Not connected to GraphTraversal, don't rush it. We can wait post TP3 GA.
public class UnionStep<S, E> extends AbstractStep<S, E> {

    public TraversalRing<S, E> traversalRing;

    @SafeVarargs
    public UnionStep(final Traversal traversal, final Traversal<S, E>... branchTraversals) {
        super(traversal);
        this.traversalRing = new TraversalRing<>(branchTraversals);
    }

    protected Traverser<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.traversalRing.size()) {
                final Traversal<S, E> branch = this.traversalRing.next();
                if (branch.hasNext()) return TraversalHelper.getEnd(branch).next();
            }
            final Traverser<S> start = this.starts.next();
            this.traversalRing.forEach(branch -> branch.addStarts(new SingleIterator<>(start.makeSibling())));
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }
}
