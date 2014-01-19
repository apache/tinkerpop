package com.tinkerpop.gremlin;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizer {

    public interface StepOptimizer extends Optimizer {
        public boolean optimize(final Pipeline pipeline, final Pipe pipe);
    }

    public interface FinalOptimizer extends Optimizer {
        public Pipeline optimize(final Pipeline pipeline);
    }

    public interface RuntimeOptimizer extends Optimizer {
        public void optimize(final Pipeline pipeline);
    }
}
