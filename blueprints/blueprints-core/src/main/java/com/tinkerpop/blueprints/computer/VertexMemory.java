package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Vertex;

/**
 * VertexMemory denotes the vertex properties that are used for the VertexProgram.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <T> Vertex.Property<T> setProperty(Vertex vertex, String key, T value);

    public <T> Vertex.Property<T> getProperty(Vertex vertex, String key);

}
