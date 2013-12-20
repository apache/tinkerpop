package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Vertex;

import java.util.Optional;

/**
 * VertexMemory denotes the vertex properties that are used for the VertexProgram.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <V> void setAnnotation(final Vertex vertex, final String key, final V value);

    public <V> Optional<V> getAnnotation(final Vertex vertex, final String key);

}
