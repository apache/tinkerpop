package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.util.optimizers.HolderOptimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinResult<T> implements Iterator<T> {

    private Iterator<T> itty;
    private final Supplier<Pipeline> gremlinSupplier;
    private final Graph graph;
    private final ComputeResult computeResult;

    public GremlinResult(final Graph graph, final Supplier<Pipeline> gremlinSupplier) {
        this.gremlinSupplier = gremlinSupplier;
        this.graph = graph;
        try {
            this.computeResult =
                    this.graph.compute()
                            .program(GremlinVertexProgram.create().gremlin((Supplier) gremlinSupplier).build())
                            .submit()
                            .get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        buildIterator();
    }

    public boolean hasNext() {
        return this.itty.hasNext();
    }

    public T next() {
        return this.itty.next();
    }

    public String toString() {
        return this.gremlinSupplier.get().toString();
    }

    private void buildIterator() {
        if (new HolderOptimizer().trackPaths(this.gremlinSupplier.get())) {
            final List list = new ArrayList();
            StreamFactory.stream(this.graph.query().vertices())
                    .map(v -> this.computeResult.getVertexMemory().<GremlinPaths>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).orElse(null))
                    .filter(t -> null != t)
                    .forEach(t -> {
                        t.getDoneObjectTracks().entrySet().stream().forEach(h -> {
                            for (int i = 0; i < h.getValue().size(); i++) {
                                list.add(h.getKey());
                            }
                        });
                        t.getDoneGraphTracks().entrySet().stream().forEach(h -> {
                            for (int i = 0; i < h.getValue().size(); i++) {
                                list.add(h.getKey());
                            }
                        });
                    });
            this.itty = list.iterator();
        } else {
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
            this.itty = list.iterator();
        }
    }
}
