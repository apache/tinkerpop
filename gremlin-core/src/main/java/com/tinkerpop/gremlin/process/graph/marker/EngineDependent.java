package com.tinkerpop.gremlin.process.graph.marker;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface EngineDependent {

    public enum Engine {STANDARD, COMPUTER}

    public void onEngine(final Engine engine);

}
