package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.strategy.BackComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.CountCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.JumpComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerHelper {

    public static void prepareTraversalForComputer(final Traversal traversal) {
        traversal.strategies().unregister(TraverserSourceStrategy.class);
        traversal.strategies().register(CountCapStrategy.instance());
        traversal.strategies().register(SideEffectCapComputerStrategy.instance());
        traversal.strategies().register(JumpComputerStrategy.instance());
        traversal.strategies().register(BackComputerStrategy.instance());
    }
}
