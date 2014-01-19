package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexVertexPipe extends AbstractVertexQueryPipe<Vertex> {

    public VertexVertexPipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder) {
        super(pipeline);
        this.queryBuilder = queryBuilder;
        this.setFunction(v -> queryBuilder.build(v.get()).vertices().iterator());
    }
}
