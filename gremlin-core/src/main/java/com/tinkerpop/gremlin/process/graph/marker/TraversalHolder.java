package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalHolder<S, E> {

    public List<Traversal<S, E>> getTraversals();

    public default Step<?, ?> asStep() {
        return (Step<?, ?>) this;
    }

}
