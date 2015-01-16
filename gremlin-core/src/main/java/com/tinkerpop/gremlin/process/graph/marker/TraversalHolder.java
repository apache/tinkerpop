package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalHolder<S, E> {

    public enum Child {
        SET_HOLDER,
        SET_SIDE_EFFECTS,
        MERGE_IN_SIDE_EFFECTS,
        SET_STRATEGIES
    }

    public List<Traversal<S, E>> getTraversals();

    public default TraversalStrategies getChildStrategies() {
        try {
            return this.asStep().getTraversal().asAdmin().getStrategies().clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public default void executeTraversalOperations(final Child... operations) {
        final List<Traversal<S, E>> traversals = this.getTraversals();
        for (final Child operation : operations) {
            for (final Traversal<S, E> traversal : traversals) {
                switch (operation) {
                    case SET_HOLDER:
                        traversal.asAdmin().setTraversalHolder(this);
                        break;
                    case MERGE_IN_SIDE_EFFECTS:
                        traversal.asAdmin().getSideEffects().mergeInto(this.asStep().getTraversal().asAdmin().getSideEffects());
                        break;
                    case SET_SIDE_EFFECTS:
                        traversal.asAdmin().setSideEffects(this.asStep().getTraversal().asAdmin().getSideEffects());
                        break;
                    case SET_STRATEGIES:
                        traversal.asAdmin().setStrategies(this.getChildStrategies());
                        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
                            if (step instanceof TraversalHolder)
                                ((TraversalHolder) step).executeTraversalOperations(Child.SET_STRATEGIES);
                        }
                        break;
                }
            }
        }
    }


    public default Set<TraverserRequirement> getTraversalRequirements(final TraverserRequirement... localRequirements) {
        final Set<TraverserRequirement> requirements = new HashSet<>();
        for (final TraverserRequirement requirement : localRequirements) {
            requirements.add(requirement);
        }
        for (final Traversal<S, E> traversals : this.getTraversals()) {
            requirements.addAll(TraversalHelper.getRequirements(traversals));
        }
        return requirements;
    }

    public default void resetTraversals() {
        this.getTraversals().forEach(traversal -> traversal.asAdmin().reset());
    }

    public default Step<?, ?> asStep() {
        return (Step<?, ?>) this;
    }

}
