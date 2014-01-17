package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexEdgePipe extends FlatMapPipe<Vertex, Edge> {

    public final VertexQueryBuilder queryBuilder;

    public VertexEdgePipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder) {
        super(pipeline, v -> queryBuilder.build(v.get()).edges().iterator());
        this.queryBuilder = queryBuilder;
    }
}
