package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexEdgePipe extends AbstractVertexQueryPipe<Edge> {

    public VertexEdgePipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder) {
        super(pipeline);
        this.queryBuilder = queryBuilder;
        this.setFunction(v -> queryBuilder.build(v.get()).edges().iterator());
    }
}
