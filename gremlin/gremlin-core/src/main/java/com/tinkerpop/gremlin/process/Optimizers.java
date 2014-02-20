package com.tinkerpop.gremlin.process;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizers {

    public List<Optimizer> get();

    public void register(final Optimizer optimizer);

    public void unregister(final Class<? extends Optimizer> optimizerClass);

    public void doFinalOptimizers(final Traversal traversal);
}
