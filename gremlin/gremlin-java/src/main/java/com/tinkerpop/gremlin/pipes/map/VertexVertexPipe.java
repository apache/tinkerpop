package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexVertexPipe extends FlatMapPipe<Vertex, Vertex> {

    public final VertexQueryBuilder queryBuilder;

    public VertexVertexPipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder) {
        super(pipeline, v -> queryBuilder.build(v.get()).vertices().iterator());
        this.queryBuilder = queryBuilder;
    }
}
