package com.tinkerpop.gremlin.giraph.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValidateStepsStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        if (((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof ShuffleStep ||
                        step instanceof AggregateStep ||
                        step instanceof DedupStep).count() > 0)
            throw new IllegalStateException("The provided traversal has steps that do not make sense in Giraph");

    }
}