package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;

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
                    graph.compute()
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
            for (final Vertex vertex : this.graph.query().vertices()) {
                StreamFactory.stream(vertex)
                        .map(v -> this.computeResult.getVertexMemory().<GremlinPaths>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).orElse(null))
                        .filter(tracker -> null != tracker)
                        .forEach(tracker -> {
                            tracker.getDoneObjectTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(holder -> {
                                    if (holder.get() instanceof Path) {
                                        list.add(Path.inflate((Path) holder.get(), this.graph));
                                    } else {
                                        list.add(holder.get());
                                    }
                                });
                            });
                            tracker.getDoneGraphTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(holder -> list.add(holder.inflate(vertex).get()));
                            });
                        });
            }
            this.itty = list.iterator();
        } else {
            final List list = new ArrayList();
            for (final Vertex vertex : this.graph.query().vertices()) {
                StreamFactory.stream(vertex)
                        .map(v -> this.computeResult.getVertexMemory().<GremlinCounters>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).orElse(null))
                        .filter(tracker -> null != tracker)
                        .forEach(tracker -> {
                            tracker.getDoneObjectTracks().entrySet().stream().forEach(entry -> {
                                for (int i = 0; i < entry.getValue(); i++) {
                                    list.add(entry.getKey().get());
                                }
                            });
                            tracker.getDoneGraphTracks().entrySet().stream().forEach(entry -> {
                                for (int i = 0; i < entry.getValue(); i++) {
                                    list.add(entry.getKey().inflate(vertex).get());
                                }
                            });
                        });
            }
            this.itty = list.iterator();
        }
    }
}
