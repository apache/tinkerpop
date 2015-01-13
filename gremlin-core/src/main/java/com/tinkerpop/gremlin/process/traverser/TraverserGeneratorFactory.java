package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.filter.CoinStep;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainStep;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGeneratorFactory {

    public TraverserGenerator getTraverserGenerator(final Traversal traversal);

    public default Set<TraverserRequirements> getRequirements(final Traversal traversal) {
        final Set<TraverserRequirements> requirements = new HashSet<>();

        requirements.add(TraverserRequirements.OBJECT);  // is get() called? -- e.g. not in g.V().count() :)

        if (!traversal.asAdmin().getSideEffects().keys().isEmpty() ||
                TraversalHelper.hasStepOfAssignableClass(ExceptStep.class, traversal) ||
                TraversalHelper.hasStepOfAssignableClass(RetainStep.class, traversal)) // check if sideEffects exist or a call to sideEffects
            requirements.add(TraverserRequirements.SIDE_EFFECTS);

        if (TraversalHelper.hasStepOfAssignableClass(RepeatStep.class, traversal) ||
                TraversalHelper.hasStepOfAssignableClass(BranchStep.class, traversal)) {
            requirements.add(TraverserRequirements.SINGLE_LOOP);
            requirements.add(TraverserRequirements.BULK);
        }

        if (TraversalHelper.trackPaths(traversal))
            requirements.add(TraverserRequirements.PATH);

        if (TraversalHelper.hasLabels(traversal))
            requirements.add(TraverserRequirements.PATH_ACCESS);

        if (traversal.asAdmin().getSideEffects().getSackInitialValue().isPresent())  // check if sack() steps exist
            requirements.add(TraverserRequirements.SACK);

        if ((traversal.asAdmin().getTraversalEngine().isPresent() && traversal.asAdmin().getTraversalEngine().get().equals(TraversalEngine.COMPUTER)) ||
                TraversalHelper.hasStepOfAssignableClass(BarrierStep.class, traversal) ||
                TraversalHelper.hasStepOfAssignableClass(RangeStep.class, traversal) ||
                TraversalHelper.hasStepOfAssignableClass(DedupStep.class, traversal) ||
                TraversalHelper.hasStepOfAssignableClass(CoinStep.class, traversal))
            requirements.add(TraverserRequirements.BULK);

        return requirements;

    }
}
