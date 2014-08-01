package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.strategy.ComputerReplacementStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerHelper {

    public static void prepareTraversalForComputer(final Traversal traversal) {
        traversal.strategies().unregister(TraverserSourceStrategy.class);
        traversal.strategies().register(ComputerReplacementStrategy.instance());
    }
}
