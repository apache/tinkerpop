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
package org.apache.tinkerpop.gremlin.structure.util.star;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;

/**
 * Kryo serializer for {@link StarGraph}.  Implements an internal versioning capability for backward compatibility.
 * The single byte at the front of the serialization stream denotes the version.  That version can be used to choose
 * the correct deserialization mechanism.  The limitation is that this versioning won't help with backward
 * compatibility for custom serializers from providers.  Providers should be encouraged to write their serializers
 * with backward compatibility in mind.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StarGraphSerializer implements SerializerShim<StarGraph> {

    private final Direction edgeDirectionToSerialize;
    private GraphFilter graphFilter;

    private final static byte VERSION_1 = Byte.MIN_VALUE;

    public StarGraphSerializer(final Direction edgeDirectionToSerialize, final GraphFilter graphFilter) {
        this.edgeDirectionToSerialize = edgeDirectionToSerialize;
        this.graphFilter = graphFilter;
    }

    @Override
    public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final StarGraph starGraph) {
        output.writeByte(VERSION_1);
        kryo.writeObjectOrNull(output, starGraph.edgeProperties, HashMap.class);
        kryo.writeObjectOrNull(output, starGraph.metaProperties, HashMap.class);
        kryo.writeClassAndObject(output, starGraph.starVertex.id);
        kryo.writeObject(output, starGraph.starVertex.label);
        writeEdges(kryo, output, starGraph, Direction.IN);
        writeEdges(kryo, output, starGraph, Direction.OUT);
        kryo.writeObject(output, null != starGraph.starVertex.vertexProperties);
        if (null != starGraph.starVertex.vertexProperties) {
            kryo.writeObject(output, starGraph.starVertex.vertexProperties.size());
            for (final Map.Entry<String, List<VertexProperty>> vertexProperties : starGraph.starVertex.vertexProperties.entrySet()) {
                kryo.writeObject(output, vertexProperties.getKey());
                kryo.writeObject(output, vertexProperties.getValue().size());
                for (final VertexProperty vertexProperty : vertexProperties.getValue()) {
                    kryo.writeClassAndObject(output, vertexProperty.id());
                    kryo.writeClassAndObject(output, vertexProperty.value());
                }
            }
        }
    }

    /**
     * If the returned {@link StarGraph} is null, that means that the {@link GraphFilter} filtered the vertex.
     */
    @Override
    public <I extends InputShim> StarGraph read(final KryoShim<I, ?> kryo, final I input, final Class<StarGraph> clazz) {
        final StarGraph starGraph = StarGraph.open();
        input.readByte();  // version field ignored for now - for future use with backward compatibility
        starGraph.edgeProperties = kryo.readObjectOrNull(input, HashMap.class);
        starGraph.metaProperties = kryo.readObjectOrNull(input, HashMap.class);
        starGraph.addVertex(T.id, kryo.readClassAndObject(input), T.label, kryo.readObject(input, String.class));
        readEdges(kryo, input, starGraph, Direction.IN);
        readEdges(kryo, input, starGraph, Direction.OUT);
        if (kryo.readObject(input, Boolean.class)) {
            final int numberOfUniqueKeys = kryo.readObject(input, Integer.class);
            for (int i = 0; i < numberOfUniqueKeys; i++) {
                final String vertexPropertyKey = kryo.readObject(input, String.class);
                final int numberOfVertexPropertiesWithKey = kryo.readObject(input, Integer.class);
                for (int j = 0; j < numberOfVertexPropertiesWithKey; j++) {
                    final Object id = kryo.readClassAndObject(input);
                    final Object value = kryo.readClassAndObject(input);
                    starGraph.starVertex.property(VertexProperty.Cardinality.list, vertexPropertyKey, value, T.id, id);
                }
            }
        }
        return this.graphFilter.hasFilter() ? starGraph.applyGraphFilter(this.graphFilter).orElse(null) : starGraph;
    }

    private <O extends OutputShim> void writeEdges(final KryoShim<?, O> kryo, final O output, final StarGraph starGraph, final Direction direction) {
        // only write edges if there are some AND if the user requested them to be serialized AND if they match
        // the direction being serialized by the format
        final Map<String, List<Edge>> starEdges = direction.equals(Direction.OUT) ? starGraph.starVertex.outEdges : starGraph.starVertex.inEdges;
        final boolean writeEdges = null != starEdges && edgeDirectionToSerialize != null
                && (edgeDirectionToSerialize == direction || edgeDirectionToSerialize == Direction.BOTH);
        kryo.writeObject(output, writeEdges);
        if (writeEdges) {
            kryo.writeObject(output, starEdges.size());
            for (final Map.Entry<String, List<Edge>> edges : starEdges.entrySet()) {
                kryo.writeObject(output, edges.getKey());
                kryo.writeObject(output, edges.getValue().size());
                for (final Edge edge : edges.getValue()) {
                    kryo.writeClassAndObject(output, edge.id());
                    kryo.writeClassAndObject(output, direction.equals(Direction.OUT) ? edge.inVertex().id() : edge.outVertex().id());
                }
            }
        }
    }

    private <I extends InputShim> void readEdges(final KryoShim<I, ?> kryo, final I input, final StarGraph starGraph, final Direction direction) {
        if (kryo.readObject(input, Boolean.class)) {
            final int numberOfUniqueLabels = kryo.readObject(input, Integer.class);
            for (int i = 0; i < numberOfUniqueLabels; i++) {
                final String edgeLabel = kryo.readObject(input, String.class);
                final int numberOfEdgesWithLabel = kryo.readObject(input, Integer.class);
                for (int j = 0; j < numberOfEdgesWithLabel; j++) {
                    final Object edgeId = kryo.readClassAndObject(input);
                    final Object adjacentVertexId = kryo.readClassAndObject(input);
                    if (this.graphFilter.checkEdgeLegality(direction, edgeLabel).positive()) {
                        if (direction.equals(Direction.OUT))
                            starGraph.starVertex.addOutEdge(edgeLabel, starGraph.addVertex(T.id, adjacentVertexId), T.id, edgeId);
                        else
                            starGraph.starVertex.addInEdge(edgeLabel, starGraph.addVertex(T.id, adjacentVertexId), T.id, edgeId);
                    } else if (null != starGraph.edgeProperties) {
                        starGraph.edgeProperties.remove(edgeId);
                    }
                }
            }
        }
    }
}


