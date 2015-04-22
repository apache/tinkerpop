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
 */
public final class StarGraphSerializer extends Serializer<StarGraph> {

    private static StarGraphSerializer INSTANCE = new StarGraphSerializer();

    private StarGraphSerializer() {

    }

    @Override
    public void write(final Kryo kryo, final Output output, final StarGraph starGraph) {
        kryo.writeObject(output, starGraph.nextId);
        kryo.writeObjectOrNull(output, starGraph.edgeProperties, HashMap.class);
        kryo.writeObjectOrNull(output, starGraph.metaProperties, HashMap.class);
        kryo.writeClassAndObject(output, starGraph.starVertex.id);
        kryo.writeObject(output, starGraph.starVertex.label);
        writeEdges(kryo, output, starGraph.starVertex.inEdges, Direction.IN);
        writeEdges(kryo, output, starGraph.starVertex.outEdges, Direction.OUT);
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
        starGraph.nextId = kryo.readObject(input, Long.class);
        starGraph.edgeProperties = kryo.readObjectOrNull(input, HashMap.class);
        starGraph.metaProperties = kryo.readObjectOrNull(input, HashMap.class);
        starGraph.addVertex(T.id, kryo.readClassAndObject(input), T.label, kryo.readObject(input, String.class));
        readEdges(kryo, input, starGraph, Direction.IN);
        readEdges(kryo, input, starGraph, Direction.OUT);
        if (kryo.readObject(input, Boolean.class)) {
            final int labelSize = kryo.readObject(input, Integer.class);
            for (int i = 0; i < labelSize; i++) {
                final String key = kryo.readObject(input, String.class);
                final int vertexPropertySize = kryo.readObject(input, Integer.class);
                for (int j = 0; j < vertexPropertySize; j++) {
                    final Object id = kryo.readClassAndObject(input);
                    final Object value = kryo.readClassAndObject(input);
                    starGraph.starVertex.property(VertexProperty.Cardinality.list, key, value, T.id, id);
                }
            }
        }
        return starGraph;
    }

    //////

    private static void writeEdges(final Kryo kryo, final Output output, final Map<String, List<Edge>> starEdges, final Direction direction) {
        kryo.writeObject(output, null != starEdges);
        if (null != starEdges) {
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
        final boolean hasEdges = kryo.readObject(input, Boolean.class);
        if (hasEdges) {
            final int labelSize = kryo.readObject(input, Integer.class);
            for (int i = 0; i < labelSize; i++) {
                final String label = kryo.readObject(input, String.class);
                final int edgeSize = kryo.readObject(input, Integer.class);
                for (int j = 0; j < edgeSize; j++) {
                    final Object edgeId = kryo.readClassAndObject(input);
                    final Object otherVertexId = kryo.readClassAndObject(input);
                    if (direction.equals(Direction.OUT))
                        starGraph.starVertex.addOutEdge(label, starGraph.addVertex(T.id, otherVertexId), T.id, edgeId);
                    else
                        starGraph.starVertex.addInEdge(label, starGraph.addVertex(T.id, otherVertexId), T.id, edgeId);
                }
            }
        }
    }

    public static StarGraphSerializer instance() {
        return INSTANCE;
    }
}
