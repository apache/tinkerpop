package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.HolderSource;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GraphStep<E extends Element> extends FlatMapStep<E, E> implements HolderSource {

    public Class<E> returnClass;

    public GraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal);
        this.returnClass = returnClass;
    }

    public abstract void generateHolderIterator(final boolean trackPaths);

    protected Holder<E> processNextStart() {
        final Holder<E> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }
}
