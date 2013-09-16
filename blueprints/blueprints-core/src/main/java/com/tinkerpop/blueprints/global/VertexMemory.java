package com.tinkerpop.blueprints.global;

import com.tinkerpop.blueprints.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public void setProperty(Vertex vertex, String key, Object value);

    public <T> T getProperty(Vertex vertex, String key);

    public <T> T removeProperty(Vertex vertex, String key);
}
