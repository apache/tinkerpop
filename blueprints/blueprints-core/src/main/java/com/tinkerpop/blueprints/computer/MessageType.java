package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MessageType {

    public static class Direct extends MessageType {
        private final GraphQuery query;
        private final Iterable<Vertex> vertices;

        private Direct(final GraphQuery query) {
            this.query = query;
            this.vertices = null;
        }

        private Direct(final Iterable<Vertex> vertices) {
            this.query = null;
            this.vertices = vertices;
        }

        public static Direct of(final GraphQuery query) {
            return new Direct(query);
        }

        public static Direct of(final Iterable<Vertex> vertices) {
            return new Direct(vertices);
        }

        public Iterable<Vertex> vertices() {
            return null == this.query ? this.vertices : this.query.vertices();
        }

        @Override
        public int hashCode() {
            return Direct.class.hashCode();
        }
    }

    public static class Adjacent extends MessageType {
        public final VertexQueryBuilder query;

        private Adjacent(final VertexQueryBuilder query) {
            this.query = query;
        }

        public static Adjacent of(final VertexQueryBuilder query) {
            return new Adjacent(query);
        }

        public Iterable<Edge> edges(final Vertex vertex) {
            return this.query.build(vertex).edges();
        }

        public Iterable<Vertex> vertices(final Vertex vertex) {
            return this.query.build(vertex).vertices();
        }

        public long count(final Vertex vertex) {
            return this.query.build(vertex).count();
        }

        public Adjacent reverse() {
            return Adjacent.of(this.query.build().reverse());
        }

        @Override
        public int hashCode() {
            return Adjacent.class.hashCode();
        }
    }
}
