package com.tinkerpop.gremlin.process.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;

import java.util.HashMap;
import java.util.Map;

/**
 * A TraversalMatrix provides random, non-linear access to the steps of a traversal by their step id.
 * This is useful in situations where traversers becomes detached from their traversal (and step) and later need to be re-attached.
 * A classic use case is {@link com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram} on {@link com.tinkerpop.gremlin.process.computer.GraphComputer}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalMatrix<S, E> {

    private final Map<String, Step<?, ?>> matrix = new HashMap<>();
    private final Traversal<S, E> traversal;

    public TraversalMatrix(final Traversal<S, E> traversal) {
        this.traversal = traversal;
        this.harvestSteps(this.traversal.asAdmin());
    }

    public <A, B, C extends Step<A, B>> C getStepById(final String stepId) {
        return (C) this.matrix.get(stepId);
    }

    public Traversal<S, E> getTraversal() {
        return this.traversal;
    }

    private final void harvestSteps(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            this.matrix.put(step.getId(), step);
            if (step instanceof TraversalHolder) {
                for (final Traversal<?, ?> nest : ((TraversalHolder) step).getGlobalTraversals()) {
                    this.harvestSteps(nest.asAdmin());
                }
            }
        }
    }

}
