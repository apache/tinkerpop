package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.util.function.SBiFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * A {@link MessageType} represents the "address" of a message. A message can have multiple receivers and message type
 * allows the underlying {@link GraphComputer} to optimize the message passing. In many situations there is no need
 * to create multiple of the same message (thus, index based on message type).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class MessageType implements Serializable {

    public static class Global extends MessageType {
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
    }

    public static class Local<M1, M2> extends MessageType {
        public final VertexQueryBuilder query;
        public final SBiFunction<M1, Edge, M2> edgeFunction;

        private Local(final VertexQueryBuilder query) {
            this.query = query;
            this.edgeFunction = (final M1 m, final Edge e) -> (M2) m;
        }

        private Local(final VertexQueryBuilder query, final SBiFunction<M1, Edge, M2> edgeFunction) {
            this.query = query;
            this.edgeFunction = edgeFunction;
        }

        public static Local of(final VertexQueryBuilder query) {
            return new Local(query);
        }

        public static <M1, M2> Local of(final VertexQueryBuilder query, final SBiFunction<M1, Edge, M2> edgeFunction) {
            return new Local<>(query, edgeFunction);
        }

        public Iterable<Edge> edges(final Vertex vertex) {
            return () -> this.query.build(vertex);
        }

        public Iterable<Vertex> vertices(final Vertex vertex) {
            return () -> this.query.build(vertex).inV();
        }

        public BiFunction<M1, Edge, M2> getEdgeFunction() {
            return this.edgeFunction;
        }

        public VertexQueryBuilder getQuery() {
            return this.query;
        }
    }
}
