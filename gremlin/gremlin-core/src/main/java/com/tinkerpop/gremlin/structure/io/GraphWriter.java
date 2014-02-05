package com.tinkerpop.gremlin.structure.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {
    public void outputGraph(final OutputStream graphMLOutputStream) throws IOException;
}
