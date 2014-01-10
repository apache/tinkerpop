package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

/**
 * VertexMemory denotes an annotatable objects annotations that are used by a VertexProgram for storing compute side-effects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <V> void setProperty(final Element element, final String key, final V value);

    public <V> Property<V> getProperty(final Element element, final String key);

}
