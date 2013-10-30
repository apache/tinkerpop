package com.tinkerpop.blueprints.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {
    public void outputGraph(OutputStream graphMLOutputStream) throws IOException;
}
