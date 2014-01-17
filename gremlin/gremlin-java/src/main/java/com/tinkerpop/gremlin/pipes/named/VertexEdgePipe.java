package com.tinkerpop.gremlin.pipes.named;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.FlatMapPipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexEdgePipe extends FlatMapPipe<Vertex, Edge> {

    public final Direction direction;
    public final String[] labels;
    public final int localLimit;

    public VertexEdgePipe(final Pipeline pipeline, final Direction direction, final int localLimit, final String... labels) {
        super(pipeline, v -> v.get().query().direction(direction).labels(labels).limit(localLimit).edges().iterator());
        this.direction = direction;
        this.labels = labels;
        this.localLimit = localLimit;
    }
}
