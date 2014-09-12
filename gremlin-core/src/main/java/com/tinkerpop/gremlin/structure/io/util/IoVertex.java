package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

/**
 * Serializable form of {@link Vertex} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoVertex extends IoElement {
    public static IoVertex from(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            throw new IllegalArgumentException(String.format("Cannot convert %s", DetachedVertex.class.getSimpleName()));

        final IoVertex iov = new IoVertex();
        return from(vertex, iov);
    }
}
