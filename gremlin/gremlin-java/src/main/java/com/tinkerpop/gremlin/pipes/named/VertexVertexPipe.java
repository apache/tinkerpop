package com.tinkerpop.gremlin.pipes.named;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.FlatMapPipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexVertexPipe extends FlatMapPipe<Vertex, Vertex> {

    public final Direction direction;
    public final String[] labels;
    public final int localLimit;

    public VertexVertexPipe(final Pipeline pipeline, final Direction direction, final int localLimit, final String... labels) {
        super(pipeline, v -> v.get().query().direction(direction).labels(labels).limit(localLimit).vertices().iterator());
        this.direction = direction;
        this.labels = labels;
        this.localLimit = localLimit;
    }
}
