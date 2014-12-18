package com.tinkerpop.gremlin.neo4j.process.graph.strategy;

import com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect.Neo4jGraphStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Pieter Martin
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphStepStrategy extends AbstractTraversalStrategy {

    private static final Neo4jGraphStepStrategy INSTANCE = new Neo4jGraphStepStrategy();
    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>();

    static {
        POSTS.add(TraverserSourceStrategy.class);
    }

    private Neo4jGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal traversal, final TraversalEngine traversalEngine) {
        if (traversal.asAdmin().getSteps().get(0) instanceof Neo4jGraphStep) {
            final Neo4jGraphStep neo4jGraphStep = (Neo4jGraphStep) traversal.asAdmin().getSteps().get(0);
            Step currentStep = neo4jGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    neo4jGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (TraversalHelper.isLabeled(currentStep)) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

    public static Neo4jGraphStepStrategy instance() {
        return INSTANCE;
    }

}
