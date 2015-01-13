package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AddEdgeStep extends SideEffectStep<Vertex> implements Reversible {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.PATH,
            TraverserRequirement.OBJECT
    ));

    // TODO: Weight key based on Traverser.getCount() ?

    private final Direction direction;
    private final String edgeLabel;
    private final String stepLabel;

    public AddEdgeStep(final Traversal traversal, final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        super(traversal);
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.stepLabel = stepLabel;
        super.setConsumer(traverser -> {
            final Vertex currentVertex = traverser.get();
            final Vertex otherVertex = traverser.path().get(stepLabel);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                otherVertex.addEdge(edgeLabel, currentVertex, propertyKeyValues);
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                currentVertex.addEdge(edgeLabel, otherVertex, propertyKeyValues);
            }
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.edgeLabel, this.stepLabel);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
