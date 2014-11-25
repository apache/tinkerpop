package com.tinkerpop.gremlin.hadoop.process.computer.example;

import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSupplier3 implements Supplier<Traversal> {
    @Override
    public Traversal get() {
        return HadoopGraph.open().V().<String>values("name").groupBy(s -> s.get().substring(1, 2), v -> v, Collection::size);
    }
}
