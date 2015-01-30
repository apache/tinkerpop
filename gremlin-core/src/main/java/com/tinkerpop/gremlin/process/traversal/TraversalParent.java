package com.tinkerpop.gremlin.process.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalParent {

    public enum Child {
        SET_HOLDER,
        SET_SIDE_EFFECTS,
        MERGE_IN_SIDE_EFFECTS,
    }

    public static Child[] TYPICAL_GLOBAL_OPERATIONS = {Child.SET_HOLDER, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS};
    public static Child[] TYPICAL_LOCAL_OPERATIONS = {Child.SET_HOLDER};

    public default <S, E> List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.emptyList();
    }

    public default <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.emptyList();
    }

    public default void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new IllegalStateException("This traversal holder does not support the addition of local traversals: " + this.getClass().getCanonicalName());
    }

    public default void addGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        throw new IllegalStateException("This traversal holder does not support the addition of global traversals: " + this.getClass().getCanonicalName());
    }

    public default void setChildStrategies(final TraversalStrategies strategies) {
        this.getGlobalChildren().forEach(traversal -> traversal.setStrategies(strategies));
        this.getLocalChildren().forEach(traversal -> traversal.setStrategies(strategies));
    }

    public default Set<TraverserRequirement> getSelfAndChildRequirements(final TraverserRequirement... currentStepRequirements) {
        final Set<TraverserRequirement> requirements = new HashSet<>();
        Collections.addAll(requirements, currentStepRequirements);
        for (final Traversal.Admin<?, ?> local : this.getLocalChildren()) {
            requirements.addAll(local.getTraverserRequirements());
        }
        for (final Traversal.Admin<?, ?> global : this.getGlobalChildren()) {
            requirements.addAll(global.getTraverserRequirements());
        }
        return requirements;
    }

    public default Step<?, ?> asStep() {
        return (Step<?, ?>) this;
    }

    public default <S, E> Traversal.Admin<S, E> integrateChild(final Traversal.Admin<?, ?> childTraversal, final TraversalParent.Child... operations) {
        for (final Child operation : operations) {
            switch (operation) {
                case SET_HOLDER:
                    childTraversal.setParent(this);
                    break;
                case MERGE_IN_SIDE_EFFECTS:
                    childTraversal.getSideEffects().mergeInto(this.asStep().getTraversal().asAdmin().getSideEffects());
                    break;
                case SET_SIDE_EFFECTS:
                    childTraversal.setSideEffects(this.asStep().getTraversal().asAdmin().getSideEffects());
                    break;
            }
        }
        return (Traversal.Admin<S, E>) childTraversal;
    }
}
