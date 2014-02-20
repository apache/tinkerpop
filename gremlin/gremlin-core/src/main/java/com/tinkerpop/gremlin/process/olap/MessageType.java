package com.tinkerpop.gremlin.process.olap;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.VertexQueryBuilder;

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
public abstract class MessageType {

    private final String label;

    public MessageType(final String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public static class Global extends MessageType {
        private final Iterable<Vertex> vertices;

        private Global(final String label) {
            super(label);
            this.vertices = null;
        }

        private Global(final String label, final Iterable<Vertex> vertices) {
            super(label);
            this.vertices = vertices;
        }

        public static Global of(final String label, final Iterable<Vertex> vertices) {
            return new Global(label, vertices);
        }

        public static Global of(final String label, final Vertex... vertices) {
            return new Global(label, Arrays.asList(vertices));
        }

        public static Global of(final String label) {
            return new Global(label);
        }

        public Iterable<Vertex> vertices() {
            return this.vertices;
        }
    }

    public static class Local<M1, M2> extends MessageType {
        public final VertexQueryBuilder query;
        public final BiFunction<M1, Edge, M2> edgeFunction;

        private Local(final String label, final VertexQueryBuilder query) {
            super(label);
            this.query = query;
            this.edgeFunction = (final M1 m, final Edge e) -> (M2) m;
        }

        private Local(final String label, final VertexQueryBuilder query, final BiFunction<M1, Edge, M2> edgeFunction) {
            super(label);
            this.query = query;
            this.edgeFunction = edgeFunction;
        }

        public static Local of(final String label, final VertexQueryBuilder query) {
            return new Local(label, query);
        }

        public static <M1, M2> Local of(final String label, final VertexQueryBuilder query, final BiFunction<M1, Edge, M2> edgeFunction) {
            return new Local<>(label, query, edgeFunction);
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
