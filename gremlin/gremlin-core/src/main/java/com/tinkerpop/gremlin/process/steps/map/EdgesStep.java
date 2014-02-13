package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesStep extends FlatMapStep<Edge, Edge> {

    public final Iterator<Edge> edges;

    public EdgesStep(final Traversal traversal, final Iterator<Edge> edges) {
        super(traversal);
        this.edges = edges;
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths) {
            this.starts.add(new HolderIterator(this, this.edges));
        } else {
            this.starts.add(new HolderIterator(this.edges));
        }
    }

    protected Holder<Edge> processNextStart() {
        final Holder<Edge> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }
}
