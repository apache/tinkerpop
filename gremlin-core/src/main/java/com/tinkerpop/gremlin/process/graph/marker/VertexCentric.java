package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexCentric {

    public void setCurrentVertex(final Vertex vertex);
}
