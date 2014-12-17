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
        return HadoopGraph.open().V().<String>values("name").group().<String>by(s -> s.substring(1, 2)).by(v -> v).<Collection>by(Collection::size);
    }
}
