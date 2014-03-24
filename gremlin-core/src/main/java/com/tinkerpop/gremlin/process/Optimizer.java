package com.tinkerpop.gremlin.process;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizer  extends Serializable {

    public interface FinalOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

    public interface RuntimeOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

}
