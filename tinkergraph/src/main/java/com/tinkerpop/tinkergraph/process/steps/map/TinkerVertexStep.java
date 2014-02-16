package com.tinkerpop.tinkergraph.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.HolderGenerator;
import com.tinkerpop.gremlin.process.steps.map.MapStep;
import com.tinkerpop.gremlin.process.steps.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexStep extends MapStep<Vertex, Vertex> implements HolderGenerator {

    public Vertex vertex;

    public TinkerVertexStep(final Traversal traversal, final Vertex vertex) {
        super(traversal);
        this.vertex = vertex;
        this.setFunction(Holder::get);
        this.generateHolderIterator(false);
    }

    public void generateHolderIterator(final boolean trackPaths) {
        final Holder<Vertex> holder = trackPaths ? new PathHolder<>(this.getAs(), this.vertex) : new SimpleHolder<>(this.vertex);
        holder.setFuture(this.getNextStep().getAs());
        this.starts.clear();
        this.starts.add(new SingleIterator(holder));
    }
}
