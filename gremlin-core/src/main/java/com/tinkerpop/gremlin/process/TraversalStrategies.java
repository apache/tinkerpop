package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalStrategies extends Serializable {

    public void register(final TraversalStrategy traversalStrategy);

    public void unregister(final Class<? extends TraversalStrategy> optimizerClass);

    public void clear();

    public void apply();

    public boolean complete();
}
