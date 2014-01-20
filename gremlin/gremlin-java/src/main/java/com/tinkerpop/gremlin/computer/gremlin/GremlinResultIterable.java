package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Pipeline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinResultIterable<T> implements Iterable<T> {

    private final ComputeResult computeResult;
    private final Graph graph;

    public GremlinResultIterable(final Graph graph, final Supplier<Pipeline> gremlinSupplier) {
        this.graph = graph;
        try {
            this.computeResult =
                    this.graph.compute().program(GremlinVertexProgram.create().gremlin((Supplier) gremlinSupplier)
                            .build())
                            .submit().get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    public Iterator<T> iterator() {
        final List list = new ArrayList();
        StreamFactory.stream(this.graph.query().vertices())
                .map(v -> this.computeResult.getVertexMemory().<GremlinCounters>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).orElse(null))
                .filter(t -> null != t)
                .forEach(t -> {
                    t.getDoneObjectTracks().entrySet().stream().forEach(h -> {
                        for (int i = 0; i < h.getValue(); i++) {
                            list.add(h.getKey().get());
                        }
                    });
                    t.getDoneGraphTracks().entrySet().stream().forEach(h -> {
                        for (int i = 0; i < h.getValue(); i++) {
                            list.add(h.getKey().get());
                        }
                    });
                });
        return list.iterator();
    }
}
