package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph.examples;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SSupplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier1 implements SSupplier<Traversal> {

    public Traversal get() {
        // return GiraphGraph.open().V().as("x").out().jump("x", h -> h.getLoops() < 10).value("name");
        return GiraphGraph.open().V().as("x").out().out().out().out().value("name");
    }
}
