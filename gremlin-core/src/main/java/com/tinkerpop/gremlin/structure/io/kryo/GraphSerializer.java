package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import com.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

/**
 * Class used to serialize graph-based objects such as vertices, edges, properties, and paths.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphSerializer {
    /**
     * Serializes any {@link Edge} implementation encountered to a {@link DetachedEdge}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class EdgeSerializer extends Serializer<Edge> {
        @Override
        public void write(final Kryo kryo, final Output output, final Edge edge) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(edge, true));
        }

        @Override
        public Edge read(final Kryo kryo, final Input input, final Class<Edge> edgeClass) {
            final Object o = kryo.readClassAndObject(input);
            return (Edge) o;
        }
    }

    /**
     * Serializes any {@link Vertex} implementation encountered to an {@link DetachedVertex}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class VertexSerializer extends Serializer<Vertex> {
        public VertexSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Vertex vertex) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertex, true));
        }

        @Override
        public Vertex read(final Kryo kryo, final Input input, final Class<Vertex> vertexClass) {
            return (Vertex) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Property} implementation encountered to an {@link DetachedProperty}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class PropertySerializer extends Serializer<Property> {
        public PropertySerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Property property) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(property));
        }

        @Override
        public Property read(final Kryo kryo, final Input input, final Class<Property> propertyClass) {
            return (Property) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link VertexProperty} implementation encountered to an {@link DetachedVertexProperty}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class VertexPropertySerializer extends Serializer<VertexProperty> {
        public VertexPropertySerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final VertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertexProperty, true));
        }

        @Override
        public VertexProperty read(final Kryo kryo, final Input input, final Class<VertexProperty> vertexPropertyClass) {
            return (VertexProperty) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Path} implementation encountered to an {@link DetachedPath}.
     *
     * @author Marko A. Rodriguez (http://markorodriguez.com)
     */
    static class PathSerializer extends Serializer<Path> {
        public PathSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Path path) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(path, false));
        }

        @Override
        public Path read(final Kryo kryo, final Input input, final Class<Path> pathClass) {
            return (Path) kryo.readClassAndObject(input);
        }

    }

    /**
     * Serializes any {@link Traverser} implementation encountered via pre-processing with {@link Traverser.Admin#detach()}.
     *
     * @author Marko A. Rodriguez (http://markorodriguez.com)
     */
    /*static class TraverserSerializer extends Serializer<Traverser.Admin> {
        public TraverserSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Traverser.Admin traverser) {
            kryo.writeClassAndObject(output, traverser.asAdmin().detach());
        }

        @Override
        public Traverser.Admin read(final Kryo kryo, final Input input, final Class<Traverser.Admin> traverser) {
            return (Traverser.Admin) kryo.readClassAndObject(input);
        }

    }*/
}
