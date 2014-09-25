package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier3 implements Supplier<Traversal> {
    @Override
    public Traversal get() {
        return GiraphGraph.open().V().<String>value("name").groupBy(s -> s.get().substring(1, 2), v -> v, Collection::size);
    }
}
