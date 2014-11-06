package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.TraverserGenerator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserSource {

    public void generateTraversers(final TraverserGenerator traverserGenerator);

    public void clear();
}
