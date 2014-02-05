package com.tinkerpop.gremlin.structure.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphReader {
    public void inputGraph(final InputStream graphInputStream) throws IOException;
}
