package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.AbstractPipe;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.ExpandablePipeIterator;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.HolderIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryPipe<S extends Element> extends AbstractPipe<S, S> {

    public final Graph graph;
    public final GraphQueryBuilder queryBuilder;
    public final Class<S> returnClass;

    public GraphQueryPipe(final Pipeline pipeline, final Graph graph, final GraphQueryBuilder queryBuilder, final Class<S> returnClass) {
        super(pipeline);
        this.queryBuilder = queryBuilder;
        this.returnClass = returnClass;
        this.graph = graph;
        this.generateHolderIterator(false);
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts = new ExpandablePipeIterator<>();
        this.starts.add(this.returnClass.equals(Vertex.class) ?
                new HolderIterator(this, this.queryBuilder.build(this.graph).vertices().iterator(), trackPaths) :
                new HolderIterator(this, this.queryBuilder.build(this.graph).edges().iterator(), trackPaths));
    }

    public Holder<S> processNextStart() {
        return this.starts.next();
    }

    public String toString() {
        if (this.queryBuilder.hasContainers.size() > 0)
            return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
        else
            return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase());
    }

}
