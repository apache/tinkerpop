package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexVertexPipe extends FlatMapPipe<Vertex, Vertex> {

    public final Direction direction;
    public final String[] labels;
    public final int branchFactor;

    public VertexVertexPipe(final Pipeline pipeline, final Direction direction, final int branchFactor, final String... labels) {
        super(pipeline, v -> v.get().query().direction(direction).labels(labels).limit(branchFactor).vertices().iterator());
        this.direction = direction;
        this.labels = labels;
        this.branchFactor = branchFactor;
    }
}
