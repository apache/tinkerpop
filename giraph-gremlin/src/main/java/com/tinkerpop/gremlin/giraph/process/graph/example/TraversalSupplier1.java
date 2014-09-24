package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier1 implements java.util.function.Supplier<Traversal>, java.io.Serializable {
    @Override
    public Traversal get() {
        return GiraphGraph.open().V().out().out().value("name");
    }
}
