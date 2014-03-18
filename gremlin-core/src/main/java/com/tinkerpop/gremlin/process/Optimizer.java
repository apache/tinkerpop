package com.tinkerpop.gremlin.process;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizer {

    public interface FinalOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

    public interface RuntimeOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

}
