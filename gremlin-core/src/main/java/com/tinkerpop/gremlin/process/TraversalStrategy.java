package com.tinkerpop.gremlin.process;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalStrategy extends Serializable {

    public interface FinalTraversalStrategy extends TraversalStrategy {
        public void apply(final Traversal traversal);
    }

    public interface RuntimeTraversalStrategy extends TraversalStrategy {
        public void apply(final Traversal traversal);
    }

}
