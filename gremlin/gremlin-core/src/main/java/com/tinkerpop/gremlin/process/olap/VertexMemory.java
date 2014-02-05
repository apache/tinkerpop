package com.tinkerpop.gremlin.process.olap;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

/**
 * {@link VertexMemory} is used by a {@link VertexProgram} for storing compute side-effects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <V> void setProperty(final Element element, final String key, final V value);

    public <V> Property<V> getProperty(final Element element, final String key);

}
