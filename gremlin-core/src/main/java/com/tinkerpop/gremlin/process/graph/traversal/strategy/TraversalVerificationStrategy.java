package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.SupplyingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

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
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        final Step<?, ?> endStep = traversal.getEndStep() instanceof ComputerAwareStep.EndStep ?
                ((ComputerAwareStep.EndStep) traversal.getEndStep()).getPreviousStep() :
                traversal.getEndStep();

        for (final Step<?, ?> step : traversal.getSteps()) {
            if ((step instanceof ReducingBarrierStep || step instanceof SupplyingBarrierStep) && (step != endStep || !(traversal.getParent() instanceof EmptyStep))) {
                throw new IllegalStateException("Global traversals on GraphComputer may not contain mid-traversal barriers: " + step);
            }
            if (step instanceof TraversalParent) {
                final Optional<Traversal.Admin<Object, Object>> traversalOptional = ((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                        .findAny();
                if (traversalOptional.isPresent())
                    throw new IllegalStateException("Local traversals on GraphComputer may not traverse past the local star-graph: " + traversalOptional.get());
            }
        }
    }

    public static TraversalVerificationStrategy instance() {
        return INSTANCE;
    }
}
