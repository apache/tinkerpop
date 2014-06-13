package com.tinkerpop.gremlin.giraph.process.graph.strategy;

import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGroupCountStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectReplacementStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof GroupCountStep)
                .collect(Collectors.<Step>toList())
                .forEach(step -> {
                    int index = TraversalHelper.removeStep(step, traversal);
                    TraversalHelper.insertStep(new GiraphGroupCountStep(traversal, ((GroupCountStep) step).functionRing.functions), index, traversal);
                });
    }
}
