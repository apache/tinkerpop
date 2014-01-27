package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.HolderIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryPipe extends FlatMapPipe<Element, Element> {

    public final Graph graph;
    public final GraphQueryBuilder queryBuilder;
    public final Class<? extends Element> returnClass;

    public GraphQueryPipe(final Pipeline pipeline, final Graph graph, final GraphQueryBuilder queryBuilder, final Class<? extends Element> returnClass) {
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
