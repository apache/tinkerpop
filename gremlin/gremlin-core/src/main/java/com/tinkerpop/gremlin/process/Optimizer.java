package com.tinkerpop.gremlin.process;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizer {

    public interface StepOptimizer extends Optimizer {
        public boolean optimize(final Traversal traversal, final Step step);
    }

    public interface FinalOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

    public interface RuntimeOptimizer extends Optimizer {
        public void optimize(final Traversal traversal);
    }

    /*public static boolean classContainedIn(final Class clazz, final List<Class> classes) {
        return classes.stream().filter(c -> c.isAssignableFrom(clazz)).findFirst().isPresent();
    }*/
}
