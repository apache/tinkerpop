package com.tinkerpop.gremlin.structure.util.wrapped;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface WrappedVertexProperty<P> {

    public P getBaseVertexProperty();
}
