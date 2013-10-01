package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <T> Property<T, Vertex> setProperty(Vertex vertex, String key, T value);

    public <T> Property<T, Vertex> getProperty(Vertex vertex, String key);

    public <T> Property<T, Vertex> removeProperty(Vertex vertex, String key);
}
