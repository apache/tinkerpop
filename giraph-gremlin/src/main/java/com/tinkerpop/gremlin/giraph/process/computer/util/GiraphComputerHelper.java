package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.strategy.CountCapStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerHelper {

    public static void prepareTraversalForComputer(final Traversal traversal) {
        traversal.strategies().register(CountCapStrategy.instance());
    }
}
