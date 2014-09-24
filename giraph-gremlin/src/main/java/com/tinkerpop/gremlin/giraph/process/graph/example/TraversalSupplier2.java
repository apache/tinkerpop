package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier2 implements java.util.function.Supplier<Traversal>, java.io.Serializable {
    @Override
    public Traversal get() {
        return GiraphGraph.open().V().<String>value("name").map(s -> s.get().length()).groupCount(i -> i.get() + 100);
    }
}
