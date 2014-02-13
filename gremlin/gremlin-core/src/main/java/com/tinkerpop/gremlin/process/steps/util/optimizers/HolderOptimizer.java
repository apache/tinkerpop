package com.tinkerpop.gremlin.process.steps.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.filter.SimplePathStep;
import com.tinkerpop.gremlin.process.steps.map.BackStep;
import com.tinkerpop.gremlin.process.steps.map.GraphQueryStep;
import com.tinkerpop.gremlin.process.steps.map.MatchStep;
import com.tinkerpop.gremlin.process.steps.map.PathStep;
import com.tinkerpop.gremlin.process.steps.map.SelectStep;
import com.tinkerpop.gremlin.process.steps.sideEffect.LinkStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer.FinalOptimizer {

    private static final List<Class> PATH_PIPES = new ArrayList<Class>(
            Arrays.asList(
                    PathStep.class,
                    BackStep.class,
                    SelectStep.class,
                    SimplePathStep.class,
                    MatchStep.class,
                    LinkStep.class));

    public void optimize(final Traversal traversal) {
        final boolean trackPaths = HolderOptimizer.trackPaths(traversal);
        traversal.getSteps().forEach(pipe -> {
            if (pipe instanceof GraphQueryStep)
                ((GraphQueryStep) pipe).generateHolderIterator(trackPaths);
        });
    }

    public static <S, E> boolean trackPaths(final Traversal<S, E> traversal) {
        return traversal.getSteps().stream()
                .filter(pipe -> PATH_PIPES.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
                .findFirst()
                .isPresent();
    }

    public static <S, E> void doPathTracking(final Traversal<S, E> traversal) {
        traversal.getSteps().forEach(pipe -> {
            if (pipe instanceof GraphQueryStep)
                ((GraphQueryStep) pipe).generateHolderIterator(true);
        });
    }
}
