package com.tinkerpop.gremlin.process.olap.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.ComputeResult;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.process.util.MicroPath;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalResult<T> implements Iterator<T> {

    private Iterator<T> itty;
    private final Supplier<Traversal> gremlinSupplier;
    private final Graph graph;
    private final ComputeResult computeResult;

    public <K, V, R> TraversalResult(final Graph graph, final Supplier<Traversal> gremlinSupplier, final BiFunction<K, Iterator<V>, R> reduction) {
        this.gremlinSupplier = gremlinSupplier;
        this.graph = graph;
        final GraphComputer computer = graph.compute();
        computer.program(TraversalVertexProgram.create().gremlin((Supplier) gremlinSupplier).build());

        try {
            this.computeResult = computer.submit().get();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        buildIterator();
    }

    public TraversalResult(final Graph graph, final Supplier<Traversal> gremlinSupplier) {
        this(graph, gremlinSupplier, null);
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
        if (HolderOptimizer.trackPaths(this.gremlinSupplier.get())) {
            this.itty = StreamFactory.stream((Iterator<Vertex>) this.graph.V()).flatMap(vertex -> {
                return StreamFactory.stream(vertex)
                        .map(v -> this.computeResult.getVertexMemory().<TraversalPaths>getProperty(v, TraversalVertexProgram.GREMLIN_TRACKER).orElse(null))
                        .filter(tracker -> null != tracker)
                        .flatMap(tracker -> {
                            final List list = new ArrayList();
                            tracker.getDoneObjectTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(holder -> {
                                    if (entry.getKey() instanceof MicroPath) {
                                        list.add(((MicroPath) entry.getKey()).inflate(this.graph));
                                    } else {
                                        list.add(entry.getKey());
                                    }
                                });
                            });
                            tracker.getDoneGraphTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(holder -> list.add(holder.inflate(vertex).get()));
                            });
                            return list.stream();
                        });
            }).iterator();
        } else {
            this.itty = StreamFactory.stream((Iterator<Vertex>) this.graph.V()).flatMap(vertex -> {
                return StreamFactory.stream(vertex)
                        .map(v -> this.computeResult.getVertexMemory().<TraversalCounters>getProperty(v, TraversalVertexProgram.GREMLIN_TRACKER).orElse(null))
                        .filter(tracker -> null != tracker)
                        .flatMap(tracker -> {
                            final List list = new ArrayList();
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
                            return list.stream();
                        });
            }).iterator();
        }
    }
}
