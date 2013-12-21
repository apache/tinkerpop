package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Element;

import java.util.Optional;

/**
 * AnnotationMemory denotes an annotatable objects annotations that are used by a VertexProgram for storing compute side-effects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotationMemory {

    public <V> void setElementAnnotation(final Element element, final String key, final V value);

    public <V> void setPropertyAnnotation(final Element element, final String propertyKey, final String key, final V value);

    public <V> Optional<V> getElementAnnotation(final Element element, final String key);

    public <V> Optional<V> getPropertyAnnotation(final Element element, final String propertyKey, final String key);

}
