package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UntilStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UntilStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final UntilStrategy INSTANCE = new UntilStrategy();
    private static final String UNTIL_PREFIX = "gremlin.until.";

    private UntilStrategy() {
    }

    // g.V.until('a'){it.object == blah}.out.out.as('a').name
    // g.V.as('a').jump('b'){it.object == blah}.out.out.jump('a').as('b').name
    @Override
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(UntilStep.class, traversal))
            return;

        int counter = 0;
        for (final UntilStep untilStep : TraversalHelper.getStepsOfClass(UntilStep.class, traversal)) {
            final IdentityStep leftEndStep = new IdentityStep(traversal);
            leftEndStep.setLabel(UNTIL_PREFIX + counter++);
            TraversalHelper.insertBeforeStep(leftEndStep, untilStep, traversal);
            final Step rightEndStep = TraversalHelper.getStep(untilStep.getBreakLabel(), traversal);
            final String rightEndLabel = rightEndStep.getLabel();

            final JumpStep leftEndJumpStep = untilStep.createLeftJumpStep(traversal, rightEndLabel);
            leftEndJumpStep.setLabel(untilStep.getLabel());
            leftEndJumpStep.doWhile = false;
            TraversalHelper.removeStep(untilStep, traversal);
            TraversalHelper.insertAfterStep(leftEndJumpStep, leftEndStep, traversal);

            final JumpStep rightEndJumpStep = untilStep.createRightJumpStep(traversal, leftEndStep.getLabel());
            //new JumpStep(traversal, leftEndStep.getLabel());
            rightEndJumpStep.setLabel(rightEndLabel);
            rightEndStep.setLabel(Graph.System.system(UUID.randomUUID().toString()));
            TraversalHelper.insertAfterStep(rightEndJumpStep, rightEndStep, traversal);
        }
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return (traversalStrategy instanceof TraverserSourceStrategy) ||
                (traversalStrategy instanceof GraphComputerStrategy) ? -1 : 0;
    }

    public static UntilStrategy instance() {
        return INSTANCE;
    }
}
