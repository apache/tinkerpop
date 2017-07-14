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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.util.Map;

/**
 * An alternative Gryo serializer that uses "referenced" graph elements during serialization. Referenced elements such
 * as {@link ReferenceVertex} exclude the label and the properties associated with it and only return the identifier.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.6, not directly replaced - supported through {@link HaltedTraverserStrategy}.
 */
@Deprecated
public class GryoLiteMessageSerializerV1d0 extends AbstractGryoMessageSerializerV1d0 {

    private static final String MIME_TYPE = SerTokens.MIME_GRYO_LITE_V1D0;
    private static final String MIME_TYPE_STRINGD = SerTokens.MIME_GRYO_LITE_V1D0 + "-stringd";

    /**
     * Creates an instance with a standard {@link GryoMapper} instance. Note that this instance
     * will be overridden by {@link #configure} is called.
     */
    public GryoLiteMessageSerializerV1d0() {
        super(overrideWithLite(GryoMapper.build()).create());
    }

    /**
     * Creates an instance with a standard {@link GryoMapper} instance. Note that the instance created by the supplied
     * builder will be overridden by {@link #configure} if it is called.
     */
    public GryoLiteMessageSerializerV1d0(final GryoMapper.Builder kryo) {
        super(overrideWithLite(kryo).create());
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }

    @Override
    GryoMapper.Builder configureBuilder(final GryoMapper.Builder builder, final Map<String, Object> config,
                                        final Map<String, Graph> graphs) {
        return overrideWithLite(builder);
    }

    private static GryoMapper.Builder overrideWithLite(final GryoMapper.Builder builder) {
        // override the core graph Elements so as to serialize with "reference" as opposed to "detached"
        builder.addCustom(Edge.class, new EdgeLiteSerializer());
        builder.addCustom(Vertex.class, new VertexLiteSerializer());
        builder.addCustom(VertexProperty.class, new VertexPropertyLiteSerializer());
        builder.addCustom(Property.class, new PropertyLiteSerializer());
        builder.addCustom(Path.class, new PathLiteSerializer());
        return builder;
    }

    /**
     * Serializes any {@link Edge} implementation encountered to a {@link ReferenceEdge}.
     */
    final static class EdgeLiteSerializer extends Serializer<Edge> {
        @Override
        public void write(final Kryo kryo, final Output output, final Edge edge) {
            kryo.writeClassAndObject(output, ReferenceFactory.detach(edge));
        }

        @Override
        public Edge read(final Kryo kryo, final Input input, final Class<Edge> edgeClass) {
            final Object o = kryo.readClassAndObject(input);
            return (Edge) o;
        }
    }

    /**
     * Serializes any {@link Vertex} implementation encountered to an {@link ReferenceVertex}.
     */
    final static class VertexLiteSerializer extends Serializer<Vertex> {
        @Override
        public void write(final Kryo kryo, final Output output, final Vertex vertex) {
            kryo.writeClassAndObject(output, ReferenceFactory.detach(vertex));
        }

        @Override
        public Vertex read(final Kryo kryo, final Input input, final Class<Vertex> vertexClass) {
            return (Vertex) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Property} implementation encountered to an {@link ReferenceProperty}.
     */
    final static class PropertyLiteSerializer extends Serializer<Property> {
        @Override
        public void write(final Kryo kryo, final Output output, final Property property) {
            kryo.writeClassAndObject(output, property instanceof VertexProperty ? ReferenceFactory.detach((VertexProperty) property) : ReferenceFactory.detach(property));
        }

        @Override
        public Property read(final Kryo kryo, final Input input, final Class<Property> propertyClass) {
            return (Property) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link VertexProperty} implementation encountered to an {@link ReferenceVertexProperty}.
     */
    final static class VertexPropertyLiteSerializer extends Serializer<VertexProperty> {
        @Override
        public void write(final Kryo kryo, final Output output, final VertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, ReferenceFactory.detach(vertexProperty));
        }

        @Override
        public VertexProperty read(final Kryo kryo, final Input input, final Class<VertexProperty> vertexPropertyClass) {
            return (VertexProperty) kryo.readClassAndObject(input);
        }
    }

    /**
     * Serializes any {@link Path} implementation encountered to an {@link ReferencePath}.
     */
    final static class PathLiteSerializer extends Serializer<Path> {
        @Override
        public void write(final Kryo kryo, final Output output, final Path path) {
            kryo.writeClassAndObject(output, ReferenceFactory.detach(path));
        }

        @Override
        public Path read(final Kryo kryo, final Input input, final Class<Path> pathClass) {
            return (Path) kryo.readClassAndObject(input);
        }
    }
}