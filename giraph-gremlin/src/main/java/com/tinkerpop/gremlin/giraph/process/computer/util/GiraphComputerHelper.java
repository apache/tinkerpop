package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.giraph.process.graph.strategy.SideEffectReplacementStrategy;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.strategy.ComputerStepReplacementStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerHelper {

    public static void prepareTraversalForComputer(final Traversal traversal) {
        traversal.strategies().register(new SideEffectReplacementStrategy());
        //traversal.strategies().unregister(SideEffectCapStrategy.class);
        //traversal.strategies().unregister(TraverserSourceStrategy.class);
        // TODO: traversal.strategies().register(new ComputerJumpStrategy());
        traversal.strategies().register(new ComputerStepReplacementStrategy());
    }
}
