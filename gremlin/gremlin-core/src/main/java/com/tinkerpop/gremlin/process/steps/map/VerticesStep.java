package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class VerticesStep extends FlatMapStep<Vertex, Vertex> {

    public VerticesStep(final Traversal traversal) {
        super(traversal);
    }

    public abstract void generateHolderIterator(final boolean trackPaths);

    protected Holder<Vertex> processNextStart() {
        final Holder<Vertex> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }
}
