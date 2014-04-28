package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Vertex;

/**
 * Serializable form of {@link Vertex} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoVertex extends IoElement {
    public static IoVertex from(final Vertex vertex) {
        final IoVertex iov = new IoVertex();
        return from(vertex, iov);
    }
}
