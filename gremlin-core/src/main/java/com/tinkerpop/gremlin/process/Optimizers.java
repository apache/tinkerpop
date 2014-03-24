package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizers extends Serializable {

    public List<Optimizer> get();

    public void register(final Optimizer optimizer);

    public void unregister(final Class<? extends Optimizer> optimizerClass);

    public void doFinalOptimizers(final Traversal traversal);
}
