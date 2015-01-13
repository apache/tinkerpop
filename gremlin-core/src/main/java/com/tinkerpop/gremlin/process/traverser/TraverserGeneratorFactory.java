package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGeneratorFactory {

    public enum TraverserRequirements {
        SIDE_EFFECTS,
        SACK,
        SINGLE_LOOP,
        NESTED_LOOP,
        UNIQUE_PATH,
        SPARSE_PATH,
        BULK
    }

    public TraverserGenerator getTraverserGenerator(final Traversal traversal);

    public default Set<TraverserRequirements> getRequirements(final Traversal traversal) {
        final Set<TraverserRequirements> requirements = new HashSet<>();
        if (TraversalHelper.hasStepOfAssignableClass(SideEffectStep.class, traversal))
            requirements.add(TraverserRequirements.SIDE_EFFECTS);
        if (TraversalHelper.hasStepOfAssignableClass(RepeatStep.class, traversal))
            requirements.add(TraverserRequirements.SINGLE_LOOP);
        if (TraversalHelper.trackPaths(traversal))
            requirements.add(TraverserRequirements.UNIQUE_PATH);
        if (traversal.asAdmin().getSideEffects().getSackInitialValue().isPresent())
            requirements.add(TraverserRequirements.SACK);
        requirements.add(TraverserRequirements.BULK);
        return requirements;

    }
}
