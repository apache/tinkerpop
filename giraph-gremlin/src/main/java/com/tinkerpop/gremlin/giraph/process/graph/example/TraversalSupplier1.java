package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SSupplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier1 implements SSupplier<Traversal> {
    public Traversal get() {
        return GiraphGraph.open().V().out().out().value("name");
    }
}
