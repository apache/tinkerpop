/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A {@link MessageScope} represents the range of a message. A message can have multiple receivers and message scope
 * allows the underlying {@link GraphComputer} to apply the message passing algorithm in whichever manner is most
 * efficient. It is best to use {@link MessageScope.Local} if possible as that provides more room for optimization by
 * providers than {@link MessageScope.Global}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class MessageScope {

    /**
     * A Global message is directed at an arbitrary vertex in the graph.
     * The recipient vertex need not be adjacent to the sending vertex.
     * This message scope should be avoided if a {@link Local} can be used.
     */
    public final static class Global extends MessageScope {

        private static final Global INSTANCE = Global.of();

        private final Iterable<Vertex> vertices;

        private Global(final Iterable<Vertex> vertices) {
            this.vertices = vertices;
        }

        public static Global of(final Iterable<Vertex> vertices) {
            return new Global(vertices);
        }

        public static Global of(final Vertex... vertices) {
            return new Global(Arrays.asList(vertices));
        }

        public Iterable<Vertex> vertices() {
            return this.vertices;
        }

        public static Global instance() {
            return INSTANCE;
        }

        @Override
        public int hashCode() {
            return 4676576;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Global;
        }
    }

    /**
     * A Local message is directed to an adjacent (or "memory adjacent") vertex.
     * The adjacent vertex set is defined by the provided {@link Traversal} that dictates how to go from the sending vertex to the receiving vertex.
     * This is the preferred message scope as it can potentially be optimized by the underlying {@link Messenger} implementation.
     * The preferred optimization is to not distribute a message object to all adjacent vertices.
     * Instead, allow the recipients to read a single message object stored at the "sending" vertex.
     * This is possible via `Traversal.reverse()`. This optimizations greatly reduces the amount of data created in the computation.
     *
     * @param <M> The {@link VertexProgram} message class
     */
    public final static class Local<M> extends MessageScope {
        public final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal;
        public final BiFunction<M, Edge, M> edgeFunction;

        private Local(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal) {
            this(incidentTraversal, (final M m, final Edge e) -> m); // the default is an identity function
        }

        private Local(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal, final BiFunction<M, Edge, M> edgeFunction) {
            this.incidentTraversal = incidentTraversal;
            this.edgeFunction = edgeFunction;
        }

        public static <M> Local<M> of(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal) {
            return new Local<>(incidentTraversal);
        }

        public static <M> Local<M> of(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal, final BiFunction<M, Edge, M> edgeFunction) {
            return new Local<>(incidentTraversal, edgeFunction);
        }

        public BiFunction<M, Edge, M> getEdgeFunction() {
            return this.edgeFunction;
        }

        public Supplier<? extends Traversal<Vertex, Edge>> getIncidentTraversal() {
            return this.incidentTraversal;
        }

        @Override
        public int hashCode() {
            return this.edgeFunction.hashCode() ^ this.incidentTraversal.get().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Local &&
                    ((Local<?>) other).incidentTraversal.get().equals(this.incidentTraversal.get()) &&
                    ((Local<?>) other).edgeFunction == this.edgeFunction;
        }

        /**
         * A helper class that can be used to generate the reverse traversal of the traversal within a {@link MessageScope.Local}.
         */
        public static class ReverseTraversalSupplier implements Supplier<Traversal<Vertex, Edge>> {

            private final MessageScope.Local<?> localMessageScope;

            public ReverseTraversalSupplier(final MessageScope.Local<?> localMessageScope) {
                this.localMessageScope = localMessageScope;
            }

            public Traversal<Vertex, Edge> get() {
                return VertexProgramHelper.reverse(this.localMessageScope.getIncidentTraversal().get().asAdmin());
            }
        }
    }
}
