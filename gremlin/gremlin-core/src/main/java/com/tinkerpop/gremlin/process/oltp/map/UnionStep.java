package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.AbstractStep;
import com.tinkerpop.gremlin.process.oltp.util.TraversalRing;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnionStep<S, E> extends AbstractStep<S, E> {

    public final TraversalRing<S, E> traversalRing;

    @SafeVarargs
    public UnionStep(final Traversal traversal, final Traversal<S, E>... traversals) {
        super(traversal);
        this.traversalRing = new TraversalRing<>(traversals);
    }

    protected Holder<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.traversalRing.size()) {
                final Traversal<S, E> p = this.traversalRing.next();
                if (p.hasNext()) return TraversalHelper.getEnd(p).next();
            }
            final Holder<S> start = this.starts.next();
            this.traversalRing.forEach(p -> p.addStarts(new SingleIterator<>(start.makeSibling())));
        }
    }
}
