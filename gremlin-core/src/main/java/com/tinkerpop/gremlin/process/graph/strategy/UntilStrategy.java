package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.map.UntilStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UntilStrategy implements TraversalStrategy {

    private static final UntilStrategy INSTANCE = new UntilStrategy();
    private static final String U = "u";

    private UntilStrategy() {
    }

    @Override
    public void apply(final Traversal traversal) {
        // g.V.until('a'){it.object == blah}.out.out.as('a').name
        // g.V.as('a').jump('b'){it.object == blah}.out.out.jump('a').as('b').name
        int counter = 0;
        for (final UntilStep untilStep : TraversalHelper.getStepsOfClass(UntilStep.class, traversal)) {
            final IdentityStep leftEndStep = new IdentityStep(traversal);
            leftEndStep.setLabel(Graph.System.system(U + counter++));
            TraversalHelper.insertBeforeStep(leftEndStep, untilStep, traversal);
            final Step rightEndStep = TraversalHelper.getStep(untilStep.jumpLabel, traversal);
            final String rightEndLabel = rightEndStep.getLabel();

            final JumpStep leftEndJumpStep = new JumpStep(traversal, rightEndLabel, untilStep.jumpPredicate, untilStep.emitPredicate);
            leftEndJumpStep.setLabel(untilStep.getLabel());
            TraversalHelper.removeStep(untilStep, traversal);
            TraversalHelper.insertAfterStep(leftEndJumpStep, leftEndStep, traversal);

            final JumpStep rightEndJumpStep = new JumpStep(traversal, leftEndStep.getLabel());
            rightEndJumpStep.setLabel(rightEndLabel);
            rightEndStep.setLabel(Graph.Key.hide(UUID.randomUUID().toString()));
            TraversalHelper.insertAfterStep(rightEndJumpStep, rightEndStep, traversal);
        }
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return (traversalStrategy instanceof TraverserSourceStrategy) ||
                (traversalStrategy instanceof UnrollJumpStrategy) ||
                (traversalStrategy instanceof GraphComputerStrategy) ? -1 : 0;
    }

    public static UntilStrategy instance() {
        return INSTANCE;
    }
}
