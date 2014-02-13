package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesStep extends FlatMapStep<Vertex, Vertex> {

    public final Iterator<Vertex> vertices;

    public VerticesStep(final Traversal traversal, final Iterator<Vertex> vertices) {
        super(traversal);
        this.vertices = vertices;
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths) {
            this.starts.add(new HolderIterator(this, this.vertices));
        } else {
            this.starts.add(new HolderIterator(this.vertices));
        }
    }

    protected Holder<Vertex> processNextStart() {
        final Holder<Vertex> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }
}
