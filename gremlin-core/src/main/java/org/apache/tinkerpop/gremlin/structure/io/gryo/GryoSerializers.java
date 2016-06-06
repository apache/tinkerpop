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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

/**
 * Class used to serialize graph-based objects such as vertices, edges, properties, and paths. These objects are
 * "detached" using {@link DetachedFactory} before serialization. These serializers present a generalized way to
 * serialize the implementations of core interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializers {

    /**
     * Serializes any {@link Edge} implementation encountered to a {@link DetachedEdge}.
     */
    final static class EdgeSerializer implements SerializerShim<Edge> {
        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, Edge edge) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(edge, true));
        }

        @Override
        public <I extends InputShim> Edge read(KryoShim<I, ?> kryo, I input, Class<Edge> edgeClass) {
            final Object o = kryo.readClassAndObject(input);
            return (Edge) o;
        }
    }

    /**
     * Serializes any {@link Vertex} implementation encountered to an {@link DetachedVertex}.
     */
    final static class VertexSerializer implements SerializerShim<Vertex> {
        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, Vertex vertex) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertex, true));
        }

        @Override
        public <I extends InputShim> Vertex read(KryoShim<I, ?> kryo, I input, Class<Vertex> vertexClass) {
            return (Vertex) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Property} implementation encountered to an {@link DetachedProperty}.
     */
    final static class PropertySerializer implements SerializerShim<Property> {
        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, Property property) {
            kryo.writeClassAndObject(output, property instanceof VertexProperty ? DetachedFactory.detach((VertexProperty) property, true) : DetachedFactory.detach(property));
        }

        @Override
        public <I extends InputShim> Property read(KryoShim<I, ?> kryo, I input, Class<Property> propertyClass) {
            return (Property) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link VertexProperty} implementation encountered to an {@link DetachedVertexProperty}.
     */
    final static class VertexPropertySerializer implements SerializerShim<VertexProperty> {
        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, VertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertexProperty, true));
        }

        @Override
        public <I extends InputShim> VertexProperty read(KryoShim<I, ?> kryo, I input, Class<VertexProperty> vertexPropertyClass) {
            return (VertexProperty) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Path} implementation encountered to an {@link DetachedPath}.
     */
    public final static class PathSerializer implements SerializerShim<Path> {
        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, Path path) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(path, false));
        }

        @Override
        public <I extends InputShim> Path read(KryoShim<I, ?> kryo, I input, Class<Path> pathClass) {
            return (Path) kryo.readClassAndObject(input);
        }
    }
}
