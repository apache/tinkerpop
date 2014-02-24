package com.tinkerpop.gremlin.structure.io.kryo;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class VertexTerminator {
    public static final VertexTerminator INSTANCE = new VertexTerminator();

    private VertexTerminator() {}
}
