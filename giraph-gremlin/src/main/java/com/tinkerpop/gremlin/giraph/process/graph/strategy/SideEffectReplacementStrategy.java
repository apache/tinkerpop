package com.tinkerpop.gremlin.giraph.process.graph.strategy;

import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphAggregateStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphCountStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGroupByStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGroupCountStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphSideEffectCapStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphStoreStep;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectReplacementStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        TraversalHelper.getStepsOfClass(GroupCountStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphGroupCountStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(GroupByStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphGroupByStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(CountStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphCountStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(StoreStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphStoreStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(AggregateStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphAggregateStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(SideEffectCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GiraphSideEffectCapStep(traversal, step), traversal));
    }

}
