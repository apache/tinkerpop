package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.Optional;

/**
 * AnnotationMemory denotes an annotatable objects annotations that are used by a VertexProgram for storing compute side-effects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotationMemory {

    public <V> void setAnnotation(final Element element, final String key, final V value);

    public <V> void setAnnotation(final Property property, final String key, final V value);

    public <V> Optional<V> getAnnotation(final Element element, final String key);

    public <V> Optional<V> getAnnotation(final Property property, final String key);

}
