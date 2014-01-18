package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.AbstractPipe;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.util.ExpandablePipeIterator;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryPipe<S extends Element> extends AbstractPipe<S, S> {

    public final Graph graph;
    public final GraphQueryBuilder graphQueryBuilder;
    public final Class<? extends Element> resultingElementClass;

    public GraphQueryPipe(final Pipeline pipeline, final Graph graph, final GraphQueryBuilder graphQueryBuilder, final Class<S> resultingElementClass) {
        super(pipeline);
        this.graphQueryBuilder = graphQueryBuilder;
        this.resultingElementClass = resultingElementClass;
        this.graph = graph;
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts = new ExpandablePipeIterator<>();
        this.starts.add(this.resultingElementClass.equals(Vertex.class) ?
                new HolderIterator(Optional.empty(), this, this.graphQueryBuilder.build(this.graph).vertices().iterator(), trackPaths) :
                new HolderIterator(Optional.empty(), this, this.graphQueryBuilder.build(this.graph).edges().iterator(), trackPaths));
    }

    public Holder<S> processNextStart() {
        return this.starts.next();
    }

}
