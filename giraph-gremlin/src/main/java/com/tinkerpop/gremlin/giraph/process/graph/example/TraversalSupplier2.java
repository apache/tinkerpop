package com.tinkerpop.gremlin.giraph.process.graph.example;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SSupplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier2 implements SSupplier<Traversal> {
    public Traversal get() {
        return GiraphGraph.open().V().<String>value("name").map(s -> s.get().length()).groupCount(i -> i.get() + 100);
    }
}
