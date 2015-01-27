package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalHolder {

    public enum Child {
        SET_HOLDER,
        SET_SIDE_EFFECTS,
        MERGE_IN_SIDE_EFFECTS,
    }

    public static Child[] TYPICAL_GLOBAL_OPERATIONS = {Child.SET_HOLDER, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS};
    public static Child[] TYPICAL_LOCAL_OPERATIONS = {Child.SET_HOLDER};

    public default <S, E> List<Traversal<S, E>> getGlobalTraversals() {
        return Collections.emptyList();
    }

    public default <S, E> List<Traversal<S, E>> getLocalTraversals() {
        return Collections.emptyList();
    }

    public default void setStrategies(final TraversalStrategies strategies) {
        this.getGlobalTraversals().forEach(traversal -> traversal.asAdmin().setStrategies(strategies));
        this.getLocalTraversals().forEach(traversal -> traversal.asAdmin().setStrategies(strategies));
    }

    public default void executeTraversalOperations(final Traversal<?, ?> traversal, final Child... operations) {
        for (final Child operation : operations) {
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
            }
        }
    }

    public default Set<TraverserRequirement> getRequirements() {
        return Stream.concat(this.getLocalTraversals().stream(), this.getGlobalTraversals().stream())
                .flatMap(t -> t.asAdmin().getTraverserRequirements().stream())
                .collect(Collectors.toSet());
    }

    public default void resetTraversals() {
        this.getGlobalTraversals().forEach(traversal -> traversal.asAdmin().reset());
        this.getLocalTraversals().forEach(traversal -> traversal.asAdmin().reset());
    }

    public default Step<?, ?> asStep() {
        return (Step<?, ?>) this;
    }
}
