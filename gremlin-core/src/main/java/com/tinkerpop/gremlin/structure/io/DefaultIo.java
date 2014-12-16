package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * Default implementation of the {@link Graph.Io} interface which overrides none of the default methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DefaultIo implements Graph.Io {
    private static final DefaultIo instance = new DefaultIo();

    private DefaultIo() {}

    public static DefaultIo instance() {
        return instance;
    }
}
