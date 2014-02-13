package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class EdgesStep extends FlatMapStep<Edge, Edge> {

    public EdgesStep(final Traversal traversal) {
        super(traversal);
    }

    public abstract void generateHolderIterator(final boolean trackPaths);

    protected Holder<Edge> processNextStart() {
        final Holder<Edge> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }
}
