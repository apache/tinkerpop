/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * A test {@link Graph} that stands in for a provider in front of a real graph. It counts every
 * iterator it hands out from {@link Graph#vertices(Object...)}, {@link Graph#edges(Object...)},
 * {@link Graph.Index#queryVertexIndex} and {@link Vertex#edges}, how many of them were closed,
 * how many {@code close()} calls they received, and how many elements were read from them. It can
 * be told to report unknown ({@code Long.MAX_VALUE}) label counts, to scan vertices in id order,
 * to fail a vertex's next {@code edges()} call, to fail a vertex scan part way, or to park the
 * calling thread in a chosen {@code edges()} call until it is interrupted.
 *
 * <p>Vertices and edges reached through it are proxies of the real elements, so that the
 * executor's later {@code edges()} calls also come through the probe. They are equal to, and
 * hash like, the real elements.</p>
 */
@SuppressWarnings("unchecked")
public final class ProbeGraph {

    private final Graph base;
    private final Graph graph;
    private final AtomicInteger opened = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();
    private final AtomicInteger closeCalls = new AtomicInteger();
    private final AtomicInteger read = new AtomicInteger();
    private final AtomicInteger edgesCalls = new AtomicInteger();
    private final AtomicInteger indexQueries = new AtomicInteger();
    private final Set<String> unknownVertexLabels = new HashSet<>();
    private final Set<Object> failNextEdgesFor = new HashSet<>();
    private boolean unknownEdgeCounts;
    private boolean verticesInIdOrder;
    private int failVertexScanAfter = -1;
    private int parkAtEdgesCall = -1;
    private CountDownLatch parked;

    public ProbeGraph(final Graph base) {
        this.base = base;
        this.graph = (Graph) Proxy.newProxyInstance(Graph.class.getClassLoader(),
                new Class<?>[]{Graph.class}, this::invokeGraph);
    }

    /** The probing graph to give the planner and executor. */
    public Graph graph() {
        return graph;
    }

    /** Makes {@code countEdgesByLabel} return {@code Long.MAX_VALUE}, the {@link Graph} default, for every label. */
    public ProbeGraph withUnknownEdgeCounts() {
        unknownEdgeCounts = true;
        return this;
    }

    /** Makes a full {@code vertices()} scan return the vertices in ascending id order, so seed order is fixed. */
    public ProbeGraph withVerticesInIdOrder() {
        verticesInIdOrder = true;
        return this;
    }

    /** Makes {@code countVerticesByLabel(label)} return {@code Long.MAX_VALUE}, the {@link Graph} default. */
    public ProbeGraph withUnknownVertexCount(final String label) {
        unknownVertexLabels.add(label);
        return this;
    }

    /** Makes the next {@code edges()} call on {@code vertex} throw {@link InjectedFailure}, once. */
    public ProbeGraph failNextEdgesCall(final Vertex vertex) {
        failNextEdgesFor.add(vertex.id());
        return this;
    }

    /**
     * Makes the next full {@code vertices()} scan throw {@link InjectedFailure} from {@code next()}
     * after it has returned {@code count} vertices, once.
     */
    public ProbeGraph failVertexScanAfter(final int count) {
        failVertexScanAfter = count;
        return this;
    }

    /**
     * Makes the {@code call}-th {@code Vertex.edges()} call, counting from 1 across all vertices,
     * count down {@code reached} and then block until the calling thread is interrupted. The
     * interrupt is then restored on the thread and the call returns its edges normally, so the
     * caller sees the interrupt at its next {@link Thread#interrupted()} check. This lets a test
     * interrupt a search at a known point without sleeping.
     */
    public ProbeGraph parkAtEdgesCall(final int call, final CountDownLatch reached) {
        parkAtEdgesCall = call;
        parked = reached;
        return this;
    }

    /** How many provider iterators the probe has handed out. */
    public int opened() {
        return opened.get();
    }

    /** How many of the handed-out provider iterators have been closed. */
    public int closed() {
        return closed.get();
    }

    /** How many {@code close()} calls the handed-out provider iterators received, repeats included. */
    public int closeCalls() {
        return closeCalls.get();
    }

    /** How many elements have been read from the handed-out provider iterators. */
    public int read() {
        return read.get();
    }

    /** How many {@code Graph.Index.queryVertexIndex} calls the probe has served. */
    public int indexQueries() {
        return indexQueries.get();
    }

    /** Asserts that every handed-out provider iterator was closed, and closed exactly once. */
    public void assertAllClosedOnce(final String message) {
        assertEquals(message + ": every provider iterator must be closed", opened(), closed());
        assertEquals(message + ": no provider iterator may be closed twice", closed(), closeCalls());
    }

    /** The exception thrown by a {@link #failNextEdgesCall(Vertex) failed} {@code edges()} call or vertex scan. */
    public static final class InjectedFailure extends RuntimeException {
        InjectedFailure(final String message) {
            super(message);
        }
    }

    // -------------------------------------------------------------------------

    private Object invokeGraph(final Object proxy, final Method method, final Object[] args) throws Throwable {
        switch (method.getName()) {
            case "vertices":
                Iterator<Vertex> vertices = (Iterator<Vertex>) forward(base, method, args);
                if (((Object[]) args[0]).length > 0)
                    return counting(vertices, this::wrapVertex);
                if (verticesInIdOrder) {
                    final List<Vertex> sorted = new ArrayList<>();
                    vertices.forEachRemaining(sorted::add);
                    CloseableIterator.closeIterator(vertices);
                    sorted.sort((x, y) -> ((Comparable<Object>) x.id()).compareTo(y.id()));
                    vertices = sorted.iterator();
                }
                if (failVertexScanAfter >= 0) {
                    vertices = new FailingAfter<>(vertices, failVertexScanAfter);
                    failVertexScanAfter = -1;
                }
                return counting(vertices, this::wrapVertex);
            case "index":
                final Graph.Index index = base.index();
                return new Graph.Index() {
                    @Override
                    public Iterator<Vertex> queryVertexIndex(final String key, final Object value) {
                        indexQueries.incrementAndGet();
                        return counting(index.queryVertexIndex(key, value), ProbeGraph.this::wrapVertex);
                    }

                    @Override
                    public long countVertexIndex(final String key, final Object value) {
                        return index.countVertexIndex(key, value);
                    }
                };
            case "edges":
                return counting((Iterator<Edge>) forward(base, method, args), this::wrapEdge);
            case "countEdgesByLabel":
                if (unknownEdgeCounts) return Long.MAX_VALUE;
                break;
            case "countVerticesByLabel":
                if (unknownVertexLabels.contains(args[0])) return Long.MAX_VALUE;
                break;
            default:
                break;
        }
        return forward(base, method, args);
    }

    private Vertex wrapVertex(final Vertex v) {
        if (v == null || Proxy.isProxyClass(v.getClass())) return v;
        return (Vertex) Proxy.newProxyInstance(Vertex.class.getClassLoader(), new Class<?>[]{Vertex.class},
                new ElementHandler(v) {
                    @Override
                    Object invokeElement(final Method method, final Object[] args) throws Throwable {
                        if (method.getName().equals("edges")) {
                            if (failNextEdgesFor.remove(v.id())) throw new InjectedFailure("injected edges() failure for vertex " + v.id());
                            if (edgesCalls.incrementAndGet() == parkAtEdgesCall) parkUntilInterrupted();
                            return counting((Iterator<Edge>) forward(v, method, args), ProbeGraph.this::wrapEdge);
                        }
                        if (method.getName().equals("vertices"))
                            return counting((Iterator<Vertex>) forward(v, method, args), ProbeGraph.this::wrapVertex);
                        return forward(v, method, args);
                    }
                });
    }

    private Edge wrapEdge(final Edge e) {
        if (e == null || Proxy.isProxyClass(e.getClass())) return e;
        return (Edge) Proxy.newProxyInstance(Edge.class.getClassLoader(), new Class<?>[]{Edge.class},
                new ElementHandler(e) {
                    @Override
                    Object invokeElement(final Method method, final Object[] args) throws Throwable {
                        final Object result = forward(e, method, args);
                        if (result instanceof Vertex) return wrapVertex((Vertex) result);
                        if (method.getName().equals("vertices"))
                            return new Mapped<>((Iterator<Vertex>) result, ProbeGraph.this::wrapVertex);
                        return result;
                    }
                });
    }

    private void parkUntilInterrupted() {
        parked.countDown();
        try {
            new CountDownLatch(1).await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private <T> Iterator<T> counting(final Iterator<T> it, final Function<T, T> wrap) {
        opened.incrementAndGet();
        return new Counting<>(it, wrap);
    }

    private static Object forward(final Object target, final Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (final InvocationTargetException ite) {
            throw ite.getCause();
        }
    }

    private static Object unwrap(final Object o) {
        if (o != null && Proxy.isProxyClass(o.getClass()) && Proxy.getInvocationHandler(o) instanceof ElementHandler)
            return ((ElementHandler) Proxy.getInvocationHandler(o)).element;
        return o;
    }

    /** Forwards {@code equals}, {@code hashCode} and {@code toString} to the real element. */
    private abstract static class ElementHandler implements InvocationHandler {
        final Element element;

        ElementHandler(final Element element) {
            this.element = element;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            switch (method.getName()) {
                case "equals":
                    return element.equals(unwrap(args[0]));
                case "hashCode":
                    return element.hashCode();
                case "toString":
                    return element.toString();
                default:
                    return invokeElement(method, args);
            }
        }

        abstract Object invokeElement(Method method, Object[] args) throws Throwable;
    }

    /** Maps elements as they are read, without counting or closing. */
    private static class Mapped<T> implements Iterator<T> {
        final Iterator<T> it;
        final Function<T, T> wrap;

        Mapped(final Iterator<T> it, final Function<T, T> wrap) {
            this.it = it;
            this.wrap = wrap;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            return wrap.apply(it.next());
        }
    }

    /** Returns {@code count} elements, then throws {@link InjectedFailure} from {@code next()}. */
    private static final class FailingAfter<T> implements Iterator<T> {
        private final Iterator<T> it;
        private int remaining;

        FailingAfter(final Iterator<T> it, final int count) {
            this.it = it;
            this.remaining = count;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            if (remaining-- == 0) throw new InjectedFailure("injected vertices() scan failure");
            return it.next();
        }
    }

    /**
     * A counted provider iterator. It counts once as closed however often it is closed, and every
     * {@code close()} call, repeats included, is counted separately in {@link #closeCalls()}.
     */
    private final class Counting<T> extends Mapped<T> implements CloseableIterator<T> {
        private boolean isClosed;

        Counting(final Iterator<T> it, final Function<T, T> wrap) {
            super(it, wrap);
        }

        @Override
        public T next() {
            final T t = super.next();
            read.incrementAndGet();
            return t;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            if (isClosed) return;
            isClosed = true;
            closed.incrementAndGet();
            CloseableIterator.closeIterator(it);
        }
    }
}
