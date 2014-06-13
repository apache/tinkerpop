package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.graph.marker.UnBulkable;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkStep extends FilterStep<Vertex> implements PathConsumer, Reversible, UnBulkable {

    public Direction direction;
    public String label;
    public String as;

    public LinkStep(final Traversal traversal, final Direction direction, final String label, final String as) {
        super(traversal);
        this.direction = direction;
        this.label = label;
        this.as = as;
        super.setPredicate(traverser -> {
            final Vertex current = traverser.get();
            final Vertex other = traverser.getPath().get(as);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                other.addEdge(label, current);
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                current.addEdge(label, other);
            }
            return true;
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.label, this.as);
    }
}
