/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StarGraphGryoSerializer extends Serializer<StarGraph> {

    private static final Map<Direction, StarGraphGryoSerializer> CACHE = new HashMap<>();

    private final Direction edgeDirectionToSerialize;

    static {
        CACHE.put(Direction.BOTH, new StarGraphGryoSerializer(Direction.BOTH));
        CACHE.put(Direction.IN, new StarGraphGryoSerializer(Direction.IN));
        CACHE.put(Direction.OUT, new StarGraphGryoSerializer(Direction.OUT));
        CACHE.put(null, new StarGraphGryoSerializer(null));
    }

    private StarGraphGryoSerializer(final Direction edgeDirectionToSerialize) {
        this.edgeDirectionToSerialize = edgeDirectionToSerialize;
    }

    /**
     * Gets a serializer from the cache.  Use {@code null} for the direction when requiring a serializer that
     * doesn't serialize the edges of a vertex.
     */
    public static StarGraphGryoSerializer with(final Direction direction) {
        return CACHE.get(direction);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final StarGraph starGraph) {
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

    @Override
    public StarGraph read(final Kryo kryo, final Input input, final Class<StarGraph> aClass) {
        final StarGraph starGraph = StarGraph.open();
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
        return starGraph;
    }

    private void writeEdges(final Kryo kryo, final Output output, final StarGraph starGraph, final Direction direction) {
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

    private static void readEdges(final Kryo kryo, final Input input, final StarGraph starGraph, final Direction direction) {
        if (kryo.readObject(input, Boolean.class)) {
            final int numberOfUniqueLabels = kryo.readObject(input, Integer.class);
            for (int i = 0; i < numberOfUniqueLabels; i++) {
                final String edgeLabel = kryo.readObject(input, String.class);
                final int numberOfEdgesWithLabel = kryo.readObject(input, Integer.class);
                for (int j = 0; j < numberOfEdgesWithLabel; j++) {
                    final Object edgeId = kryo.readClassAndObject(input);
                    final Object adjacentVertexId = kryo.readClassAndObject(input);
                    if (direction.equals(Direction.OUT))
                        starGraph.starVertex.addOutEdge(edgeLabel, starGraph.addVertex(T.id, adjacentVertexId), T.id, edgeId);
                    else
                        starGraph.starVertex.addInEdge(edgeLabel, starGraph.addVertex(T.id, adjacentVertexId), T.id, edgeId);
                }
            }
        }
    }
}
