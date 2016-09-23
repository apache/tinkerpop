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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
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
    public final static class EdgeSerializer implements SerializerShim<Edge> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Edge edge) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(edge, true));
        }

        @Override
        public <I extends InputShim> Edge read(final KryoShim<I, ?> kryo, final I input, final Class<Edge> edgeClass) {
            final Object o = kryo.readClassAndObject(input);
            return (Edge) o;
        }
    }

    /**
     * Serializes any {@link Vertex} implementation encountered to an {@link DetachedVertex}.
     */
    public final static class VertexSerializer implements SerializerShim<Vertex> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Vertex vertex) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertex, true));
        }

        @Override
        public <I extends InputShim> Vertex read(final KryoShim<I, ?> kryo, final I input, final Class<Vertex> vertexClass) {
            return (Vertex) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Property} implementation encountered to an {@link DetachedProperty}.
     */
    public final static class PropertySerializer implements SerializerShim<Property> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Property property) {
            kryo.writeClassAndObject(output, property instanceof VertexProperty ? DetachedFactory.detach((VertexProperty) property, true) : DetachedFactory.detach(property));
        }

        @Override
        public <I extends InputShim> Property read(final KryoShim<I, ?> kryo, final I input, final Class<Property> propertyClass) {
            return (Property) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link VertexProperty} implementation encountered to an {@link DetachedVertexProperty}.
     */
    public final static class VertexPropertySerializer implements SerializerShim<VertexProperty> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final VertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(vertexProperty, true));
        }

        @Override
        public <I extends InputShim> VertexProperty read(final KryoShim<I, ?> kryo, final I input, final Class<VertexProperty> vertexPropertyClass) {
            return (VertexProperty) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Path} implementation encountered to an {@link DetachedPath}.
     */
    public final static class PathSerializer implements SerializerShim<Path> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Path path) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(path, false));
        }

        @Override
        public <I extends InputShim> Path read(final KryoShim<I, ?> kryo, final I input, final Class<Path> pathClass) {
            return (Path) kryo.readClassAndObject(input);
        }
    }

    public final static class BytecodeSerializer implements SerializerShim<Bytecode> {
        private static final GraphSONMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .addCustomModule(GraphSONXModuleV2d0.build().create(false))
                .create();

        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Bytecode bytecode) {
            try {
                output.writeString(mapper.createMapper().writeValueAsString(bytecode));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public <I extends InputShim> Bytecode read(final KryoShim<I, ?> kryo, final I input, final Class<Bytecode> clazz) {
            try {
                return mapper.createMapper().readValue(input.readString(), Bytecode.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public final static class DefaultRemoteTraverserSerializer implements SerializerShim<DefaultRemoteTraverser> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final DefaultRemoteTraverser remoteTraverser) {
            kryo.writeClassAndObject(output, remoteTraverser.get());
            output.writeLong(remoteTraverser.bulk());
        }

        @Override
        public <I extends InputShim> DefaultRemoteTraverser read(final KryoShim<I, ?> kryo, final I input, final Class<DefaultRemoteTraverser> remoteTraverserClass) {
            final Object o = kryo.readClassAndObject(input);
            return new DefaultRemoteTraverser<>(o, input.readLong());
        }
    }
}
