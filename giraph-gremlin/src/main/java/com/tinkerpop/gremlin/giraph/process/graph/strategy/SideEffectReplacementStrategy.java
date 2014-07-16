package com.tinkerpop.gremlin.giraph.process.graph.strategy;

import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphCountStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGroupByStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGroupCountStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphSideEffectCapStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphStoreStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
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
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphGroupCountStep(traversal, (GroupCountStep) step), traversal));

        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof GroupByStep)
                .collect(Collectors.<Step>toList())
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphGroupByStep(traversal, (GroupByStep) step), traversal));

        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof CountStep)
                .collect(Collectors.<Step>toList())
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphCountStep(traversal, (CountStep) step), traversal));

        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof StoreStep)
                .collect(Collectors.<Step>toList())
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphStoreStep(traversal, (StoreStep) step), traversal));

        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof SideEffectCapStep)
                .collect(Collectors.<Step>toList())
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphSideEffectCapStep(traversal, (SideEffectCapStep) step), traversal));
    }

}
