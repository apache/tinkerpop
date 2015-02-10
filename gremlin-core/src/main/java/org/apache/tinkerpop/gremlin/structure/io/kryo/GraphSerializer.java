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
package org.apache.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.tinkerpop.gremlin.process.Path;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

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
