package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexVerticesStep extends FlatMapStep<Vertex, Vertex> {

    public String[] labels;
    public Direction direction;
    public int branchFactor;

    public VertexVerticesStep(final Traversal traversal, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal);
        this.direction = direction;
        this.labels = labels;
        this.branchFactor = branchFactor;
        this.setFunction(holder -> {
            if (this.direction.equals(Direction.OUT)) {
                return holder.get().out(branchFactor, this.labels);
            } else if (this.direction.equals(Direction.IN)) {
                return holder.get().in(branchFactor, this.labels);
            } else {
                return holder.get().both(branchFactor, this.labels);
            }
        });
    }
}
