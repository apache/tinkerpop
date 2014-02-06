package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.process.util.GremlinHelper;
import com.tinkerpop.gremlin.process.util.HolderIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryPipe extends FlatMapPipe<Element, Element> {

    public final Graph graph;
    public final GraphQueryBuilder queryBuilder;
    public final Class<? extends Element> returnClass;

    public GraphQueryPipe(final Traversal pipeline, final Graph graph, final GraphQueryBuilder queryBuilder, final Class<? extends Element> returnClass) {
        super(pipeline);
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
        holder.setFuture(this.getNextPipe().getAs());
        return holder;
    }

    public String toString() {
        if (this.queryBuilder.hasContainers.size() > 0)
            return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
        else
            return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase());
    }

}
