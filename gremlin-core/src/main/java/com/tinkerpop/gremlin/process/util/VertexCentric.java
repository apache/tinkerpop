package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexCentric {

    public void setCurrentVertex(final Vertex vertex);
}
