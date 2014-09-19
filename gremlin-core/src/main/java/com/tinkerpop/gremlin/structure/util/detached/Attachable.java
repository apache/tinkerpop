package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * An interface that provides methods for detached properties and elements to be re-attached to the {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Attachable<T> {
    public abstract T attach(final Vertex hostVertex) throws IllegalStateException;

    public abstract T attach(final Graph hostGraph) throws IllegalStateException;
}
