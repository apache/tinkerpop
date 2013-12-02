package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;

import java.util.function.BiFunction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MessageType {

    private final String label;

    public MessageType(final String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public static class Direct extends MessageType {
        private final GraphQuery query;
        private final Iterable<Vertex> vertices;

        private Direct(final String label, final GraphQuery query) {
            super(label);
            this.query = query;
            this.vertices = null;
        }

        private Direct(final String label, final Iterable<Vertex> vertices) {
            super(label);
            this.query = null;
            this.vertices = vertices;
        }

        public static Direct of(final String label, final GraphQuery query) {
            return new Direct(label, query);
        }

        public static Direct of(final String label, final Iterable<Vertex> vertices) {
            return new Direct(label, vertices);
        }

        public Iterable<Vertex> vertices() {
            return null == this.query ? this.vertices : this.query.vertices();
        }
    }

    public static class Adjacent<M1, M2> extends MessageType {
        public final VertexQueryBuilder query;
        public final BiFunction<Edge, M1, M2> edgeFunction;

        private Adjacent(final String label, final VertexQueryBuilder query) {
            super(label);
            this.query = query;
            this.edgeFunction = (Edge e, M1 m) -> (M2) m;
        }

        private Adjacent(final String label, final VertexQueryBuilder query, final BiFunction<Edge, M1, M2> edgeFunction) {
            super(label);
            this.query = query;
            this.edgeFunction = edgeFunction;
        }

        public static Adjacent of(final String label, final VertexQueryBuilder query) {
            return new Adjacent(label, query);
        }

        public Iterable<Edge> edges(final Vertex vertex) {
            return this.query.build(vertex).edges();
        }

        public Iterable<Vertex> vertices(final Vertex vertex) {
            return this.query.build(vertex).vertices();
        }

        public BiFunction<Edge, M1, M2> getEdgeFunction() {
            return this.edgeFunction;
        }

        public VertexQueryBuilder getQuery() {
            return this.query;
        }
    }
}
