package com.tinkerpop.gremlin.structure.io;

/**
 * Represents a low-level serialization class that can be used to map classes to serializers.  These implementation
 * create instances of serializers from other libraries (e.g. creating a {@code Kryo} instance).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Mapper<T> {
    /**
     * Create a new instance of the internal object mapper that an implementation represents.
     */
    public T createMapper();
}
