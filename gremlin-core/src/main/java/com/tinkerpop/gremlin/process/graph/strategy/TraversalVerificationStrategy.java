package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.graph.step.util.SupplyingBarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVerificationStrategy extends AbstractTraversalStrategy {

    private static final TraversalVerificationStrategy INSTANCE = new TraversalVerificationStrategy();

    private TraversalVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {

        final Step<?, ?> endStep = traversal.getEndStep() instanceof ComputerAwareStep.EndStep ?
                ((ComputerAwareStep.EndStep) traversal.getEndStep()).getPreviousStep() :
                traversal.getEndStep();

        for (final Step<?, ?> step : traversal.getSteps()) {
            // STANDARD AND COMPUTER
            if (step instanceof TraversalHolder) {
                for (final Traversal<?, ?> global : ((TraversalHolder) step).getGlobalTraversals()) {
                    if (TraversalHelper.hasStepOfAssignableClass(ReducingBarrierStep.class, global.asAdmin()) || TraversalHelper.hasStepOfAssignableClass(SupplyingBarrierStep.class, global.asAdmin()))
                        throw new IllegalStateException("Global nested traversals may not contain barrier-steps: " + global);
                }
            }
            /// COMPUTER ONLY
            if (engine.equals(TraversalEngine.COMPUTER)) {
                if ((step instanceof ReducingBarrierStep || step instanceof SupplyingBarrierStep) && step != endStep) {
                    throw new IllegalStateException("GraphComputer traversals may not contain mid-traversal barriers: " + step);
                }
                if (step instanceof TraversalHolder) {
                    final Optional<Traversal<Object, Object>> traversalOptional = ((TraversalHolder) step).getLocalTraversals().stream()
                            .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                            .findAny();
                    if (traversalOptional.isPresent())
                        throw new IllegalStateException("Local traversals on GraphComputer may not traverse past the local star-graph: " + traversalOptional.get());
                }
            }
        }
    }

    public static TraversalVerificationStrategy instance() {
        return INSTANCE;
    }
}
