package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.TraversalEngine;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface EngineDependent {

    public void onEngine(final TraversalEngine traversalEngine);

}
