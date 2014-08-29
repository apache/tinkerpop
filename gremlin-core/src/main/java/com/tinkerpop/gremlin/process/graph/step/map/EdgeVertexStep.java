package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeVertexStep extends FlatMapStep<Edge, Vertex> implements Reversible {

    protected Direction direction;

    public EdgeVertexStep(final Traversal traversal, final Direction direction) {
        super(traversal);
        this.direction = direction;
        this.setFunction(traverser -> traverser.get().vertices(this.direction));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction);
    }

    @Override
    public void reverse() {
        this.direction = this.direction.opposite();
    }
}
