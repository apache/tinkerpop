package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SSupplier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgramIterator<T> implements Iterator<T> {

    protected Iterator<T> itty;
    protected final Supplier<Traversal> traversalSupplier;
    protected final Graph originalGraph;
    protected final Graph resultantGraph;

    protected final boolean oneNext;
    protected boolean oneDone = false;

    public TraversalVertexProgramIterator(final Graph originalGraph, final SSupplier<Traversal> traversalSupplier) {
        this.traversalSupplier = traversalSupplier;
        this.originalGraph = originalGraph;
        final GraphComputer computer = this.originalGraph.compute();
        computer.program(TraversalVertexProgram.create().traversal(traversalSupplier).getConfiguration());
        try {
            this.resultantGraph = computer.submit().get().getValue0();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        buildIterator();

        final Traversal traversal = traversalSupplier.get();
        traversal.strategies().applyFinalStrategies();
        this.oneNext = TraversalHelper.getEnd(traversal) instanceof SideEffectCapStep;
    }

    public boolean hasNext() {
        if (this.oneNext) {
            if (!this.oneDone)
                return this.itty.hasNext();
            else
                return false;
        } else
            return this.itty.hasNext();
    }

    public T next() {
        if (this.oneNext) {
            if (!this.oneDone) {
                this.oneDone = true;
                return this.itty.next();
            } else {
                throw FastNoSuchElementException.instance();
            }
        } else {
            return this.itty.next();
        }
    }

    public String toString() {
        return this.traversalSupplier.get().toString();
    }

    public Graph getResultantGraph() {
        return this.resultantGraph;
    }

    protected void buildIterator() {
        if (TraversalHelper.trackPaths(this.traversalSupplier.get())) {
            this.itty = StreamFactory.stream((Iterator<Vertex>) this.originalGraph.V()).flatMap(vertex -> {
                return StreamFactory.stream(vertex)
                        .map(v -> this.resultantGraph.v(v.id()).<TraverserPathTracker>property(TraversalVertexProgram.TRAVERSER_TRACKER).orElse(null))
                        .filter(tracker -> null != tracker)
                        .flatMap(tracker -> {
                            final List list = new ArrayList();
                            tracker.getDoneObjectTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(traverser -> {
                                    if (entry.getKey() instanceof DetachedPath) {
                                        list.add(((DetachedPath) entry.getKey()).attach(this.originalGraph));
                                    } else {
                                        list.add(entry.getKey());
                                    }
                                });
                            });
                            tracker.getDoneGraphTracks().entrySet().stream().forEach(entry -> {
                                entry.getValue().forEach(traverser -> list.add(traverser.inflate(vertex).get()));
                            });
                            return list.stream();
                        });
            }).iterator();
        } else {
            this.itty = StreamFactory.stream((Iterator<Vertex>) this.originalGraph.V()).flatMap(vertex -> {
                return StreamFactory.stream(vertex)
                        .map(v -> this.resultantGraph.v(v.id()).<TraverserCountTracker>property(TraversalVertexProgram.TRAVERSER_TRACKER).orElse(null))
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
