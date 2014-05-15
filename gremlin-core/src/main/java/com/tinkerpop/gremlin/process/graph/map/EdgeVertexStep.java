package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeVertexStep extends FlatMapStep<Edge, Vertex> {

    public Direction direction;

    public EdgeVertexStep(final Traversal traversal, final Direction direction) {
        super(traversal);
        this.direction = direction;
        this.setFunction(holder -> {
            if (direction.equals(Direction.IN))
                return holder.get().inV();
            else if (direction.equals(Direction.OUT))
                return holder.get().outV();
            else
                return holder.get().bothV();
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction);
    }
}
