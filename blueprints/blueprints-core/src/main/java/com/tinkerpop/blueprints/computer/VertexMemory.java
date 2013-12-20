package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

/**
 * VertexMemory denotes the vertex properties that are used for the VertexProgram.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <V> void setProperty(final Vertex vertex, final String key, final V value);

    public <V> Property<V> getProperty(final Vertex vertex, final String key);

}
