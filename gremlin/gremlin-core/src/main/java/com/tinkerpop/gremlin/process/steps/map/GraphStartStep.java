package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.GraphQueryBuilder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStartStep extends FlatMapStep<Element, Element> {

    public final Graph graph;
    public final GraphQueryBuilder queryBuilder;
    public final Class<? extends Element> returnClass;

    public GraphStartStep(final Traversal traversal, final Graph graph, final GraphQueryBuilder queryBuilder, final Class<? extends Element> returnClass) {
        super(traversal);
        this.queryBuilder = queryBuilder;
        this.returnClass = returnClass;
        this.graph = graph;
        this.generateHolderIterator(false);
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths) {
            this.starts.add(this.returnClass.equals(Vertex.class) ?
                    new HolderIterator(this, this.queryBuilder.build(this.graph).vertices().iterator()) :
                    new HolderIterator(this, this.queryBuilder.build(this.graph).edges().iterator()));
        } else {
            this.starts.add(this.returnClass.equals(Vertex.class) ?
                    new HolderIterator(this.queryBuilder.build(this.graph).vertices().iterator()) :
                    new HolderIterator(this.queryBuilder.build(this.graph).edges().iterator()));
        }
    }

    protected Holder<Element> processNextStart() {
        final Holder<Element> holder = this.starts.next();
        holder.setFuture(this.getNextStep().getAs());
        return holder;
    }

    public String toString() {
        if (this.queryBuilder.hasContainers.size() > 0)
            return TraversalHelper.makeStepString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
        else
            return TraversalHelper.makeStepString(this, this.returnClass.getSimpleName().toLowerCase());
    }

}
