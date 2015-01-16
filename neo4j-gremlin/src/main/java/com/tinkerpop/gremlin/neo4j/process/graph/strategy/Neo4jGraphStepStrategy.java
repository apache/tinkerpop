package com.tinkerpop.gremlin.neo4j.process.graph.strategy;

import com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect.Neo4jGraphStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Pieter Martin
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphStepStrategy extends AbstractTraversalStrategy {

    private static final Neo4jGraphStepStrategy INSTANCE = new Neo4jGraphStepStrategy();

    private Neo4jGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        if (TraversalHelper.getStart(traversal) instanceof Neo4jGraphStep) {
            final Neo4jGraphStep neo4jGraphStep = (Neo4jGraphStep) traversal.asAdmin().getSteps().get(0);
            Step<?, ?> currentStep = neo4jGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    neo4jGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static Neo4jGraphStepStrategy instance() {
        return INSTANCE;
    }

}
