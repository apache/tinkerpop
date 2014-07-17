package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnrollJumpStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        final List<JumpStep> list = (List) ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof JumpStep)
                .collect(Collectors.toList());


    }
}
