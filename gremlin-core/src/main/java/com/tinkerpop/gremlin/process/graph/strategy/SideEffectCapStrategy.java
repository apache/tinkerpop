package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapStrategy implements TraversalStrategy.NoDependencies {

    private static final SideEffectCapStrategy INSTANCE = new SideEffectCapStrategy();

    private SideEffectCapStrategy() {
    }


    public void apply(final Traversal traversal) {
        if (TraversalHelper.getEnd(traversal) instanceof SideEffectCapable)
            traversal.cap(SideEffectCapable.CAP_KEY);
    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        if (traversalStrategy instanceof TraverserSource)
            return -1;
        else if (traversalStrategy instanceof CountCapStrategy)
            return 1;
        else
            return 1;
    }

    public static SideEffectCapStrategy instance() {
        return INSTANCE;
    }

}
