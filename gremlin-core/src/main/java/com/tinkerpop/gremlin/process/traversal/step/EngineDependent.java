package com.tinkerpop.gremlin.process.traversal.step;

import com.tinkerpop.gremlin.process.TraversalEngine;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface EngineDependent {

    public void onEngine(final TraversalEngine traversalEngine);

}
