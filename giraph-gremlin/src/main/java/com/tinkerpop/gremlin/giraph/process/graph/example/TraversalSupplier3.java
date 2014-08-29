package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SSupplier;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier3 implements SSupplier<Traversal> {
    @Override
    public Traversal get() {
        return GiraphGraph.open().V().<String>value("name").groupBy(s -> s.get().substring(1, 2), v -> v, Collection::size);
    }
}
