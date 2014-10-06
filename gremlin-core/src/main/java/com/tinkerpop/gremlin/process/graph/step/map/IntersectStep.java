package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO: Not connected to GraphTraversal, don't rush it. We can wait post TP3 GA.
public class IntersectStep<S, E> extends AbstractStep<S, E> {

    private final TraversalRing<S, E> traversalRing;
    private boolean drainState = false; // TODO: Make an AtomicBoolean?

    @SafeVarargs
    public IntersectStep(final Traversal traversal, final Traversal<S, E>... traversals) {
        super(traversal);
        this.traversalRing = new TraversalRing<>(traversals);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.drainState) {
                int counter = 0;
                while (counter++ < this.traversalRing.size()) {
                    final Traversal<S, E> traversal = this.traversalRing.next();
                    if (traversal.hasNext()) {
                        return TraversalHelper.getEnd(traversal).next();
                    }
                }
                this.drainState = false;
                this.traversalRing.reset();
            } else {
                final Traverser.System<S> start = this.starts.next();
                this.traversalRing.forEach(p -> p.addStarts(new SingleIterator<>(start.makeSibling())));
                if (this.traversalRing.stream().map(p -> p.hasNext()).reduce(true, (a, b) -> a && b))
                    this.drainState = true;
                else
                    this.traversalRing.stream().forEach(TraversalHelper::iterate);
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }
}
