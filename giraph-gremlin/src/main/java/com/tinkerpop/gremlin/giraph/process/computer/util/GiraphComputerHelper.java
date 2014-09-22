package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.strategy.CountCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.UnrollJumpStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphComputerHelper {

    public static void prepareTraversalForComputer(final Traversal traversal) {
        traversal.strategies().unregister(UnrollJumpStrategy.class);
        traversal.strategies().register(CountCapStrategy.instance());
    }
}
