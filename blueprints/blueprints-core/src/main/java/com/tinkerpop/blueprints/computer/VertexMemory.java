package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.Optional;

/**
 * VertexMemory denotes an annotatable objects annotations that are used by a VertexProgram for storing compute side-effects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexMemory {

    public <V> void setProperty(final Element element, final String key, final V value);

    public <V> void setAnnotation(final Property property, final String key, final V value);

    public <V> Property<V> getProperty(final Element element, final String key);

    public <V> Optional<V> getAnnotation(final Property property, final String key);

    public void removeProperty(final Element element, final String key);

    public void removeAnnotation(final Property property, final String key);

}
