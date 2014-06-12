package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.UnBulkable;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkStep extends MapStep<Vertex, Vertex> implements PathConsumer, Reversible, UnBulkable {

    public Direction direction;
    public String label;
    public String as;

    public LinkStep(final Traversal traversal, final Direction direction, final String label, final String as) {
        super(traversal);
        this.direction = direction;
        this.label = label;
        this.as = as;
        super.setFunction(traverser -> {
            final Vertex current = traverser.get();
            final Vertex other = traverser.getPath().get(as);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                other.addEdge(label, current);
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                current.addEdge(label, other);
            }
            return current;
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.label, this.as);
    }
}
