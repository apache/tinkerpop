package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraverserSource;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GraphStep<E extends Element> extends FlatMapStep<E, E> implements TraverserSource {

    public Class<E> returnClass;

    public GraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal);
        this.returnClass = returnClass;
    }

    public abstract void generateTraverserIterator(final boolean trackPaths);

    protected Traverser<E> processNextStart() {
        final Traverser<E> traverser = this.starts.next();
        traverser.setFuture(this.getNextStep().getAs());
        return traverser;
    }
}
