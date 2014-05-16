package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.util.HolderOptimizer;
import com.tinkerpop.gremlin.process.util.MicroPath;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SSupplier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalResult<T> implements Iterator<T> {

    private Iterator<T> itty;
    private final Supplier<Traversal> traversalSupplier;
    private final Graph graph;
    private final Graph result;

    public TraversalResult(final Graph graph, final SSupplier<Traversal> traversalSupplier) {
        this.traversalSupplier = traversalSupplier;
        this.graph = graph;
        final GraphComputer computer = this.graph.compute();
        computer.program(TraversalVertexProgram.create().traversal(traversalSupplier).getConfiguration());
        try {
            this.result = computer.submit().get().getValue0();
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
        return this.traversalSupplier.get().toString();
    }

    private void buildIterator() {
        if (HolderOptimizer.trackPaths(this.traversalSupplier.get())) {
            this.itty = StreamFactory.stream((Iterator<Vertex>) this.graph.V()).flatMap(vertex -> {
                return StreamFactory.stream(vertex)
                        .map(v -> this.result.v(v.id()).<TraversalPaths>property(TraversalVertexProgram.TRAVERSAL_TRACKER).orElse(null))
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
                        .map(v -> this.result.v(v.id()).<TraversalCounters>property(TraversalVertexProgram.TRAVERSAL_TRACKER).orElse(null))
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
