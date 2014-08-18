package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddEdgeStep extends FilterStep<Vertex> implements PathConsumer, Reversible {

    public Direction direction;
    public String label;
    public String as;

    public AddEdgeStep(final Traversal traversal, final Direction direction, final String label, final String as, final Object... propertyKeyValues) {
        super(traversal);
        this.direction = direction;
        this.label = label;
        this.as = as;
        super.setPredicate(traverser -> {
            final Vertex current = traverser.get();
            final Vertex other = traverser.getPath().get(as);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                final Edge edge = other.addEdge(label, current);
                if (propertyKeyValues.length > 0) {
                    ElementHelper.attachProperties(edge, propertyKeyValues);
                }
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                final Edge edge = current.addEdge(label, other);
                if (propertyKeyValues.length > 0) {
                    ElementHelper.attachProperties(edge, propertyKeyValues);
                }
            }
            return true;
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.label, this.as);
    }
}
