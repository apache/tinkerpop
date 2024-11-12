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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSerializer extends SimpleTypeSerializer<Graph> {

    private static final Method openMethod = detectGraphOpenMethod();

    public GraphSerializer() {
        super(DataType.GRAPH);
    }

    @Override
    protected Graph readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {

        if (null == openMethod)
            throw new IOException("TinkerGraph is an optional dependency to gremlin-driver - if deserializing Graph instances it must be explicitly added as a dependency");

        final Configuration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.defaultVertexPropertyCardinality", "list");

        try {
            final Graph graph = (Graph) openMethod.invoke(null, conf);
            final int vertexCount = context.readValue(buffer, Integer.class, false);
            for (int ix = 0; ix < vertexCount; ix++) {
                final Vertex v = graph.addVertex(T.id, context.read(buffer), T.label, context.readValue(buffer, List.class, false).get(0));
                final int vertexPropertyCount = context.readValue(buffer, Integer.class, false);

                for (int iy = 0; iy < vertexPropertyCount; iy++) {
                    final Object id = context.read(buffer);
                    // reading single string value for now according to GraphBinaryV4
                    final String label = (String) context.readValue(buffer, List.class, false).get(0);
                    final Object val = context.read(buffer);
                    context.read(buffer); // toss parent as it's always null
                    final VertexProperty<Object> vp = v.property(VertexProperty.Cardinality.list, label, val, T.id, id);

                    final List<Property> edgeProperties = context.readValue(buffer, List.class, false);
                    for (Property p : edgeProperties) {
                        vp.property(p.key(), p.value());
                    }
                }
            }

            final int edgeCount = context.readValue(buffer, Integer.class, false);
            for (int ix = 0; ix < edgeCount; ix++) {
                final Object id = context.read(buffer);
                // reading single string value for now according to GraphBinaryV4
                final String label = (String) context.readValue(buffer, List.class, false).get(0);
                final Object inId = context.read(buffer);
                final Vertex inV = graph.vertices(inId).next();
                context.read(buffer);  // toss in label - always null in this context
                final Object outId = context.read(buffer);
                final Vertex outV = graph.vertices(outId).next();
                context.read(buffer); // toss in label - always null in this context
                context.read(buffer); // toss parent - never present as it's just a placeholder

                final Edge e = outV.addEdge(label, inV, T.id,  id);

                final List<Property> edgeProperties = context.readValue(buffer, ArrayList.class, false);
                for (Property p : edgeProperties) {
                    e.property(p.key(), p.value());
                }
            }

            return graph;
        } catch (Exception ex) {
            // famous last words - can't happen
            throw new IOException(ex);
        }
    }

    @Override
    protected void writeValue(final Graph value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        // this kinda looks scary memory-wise, but GraphBinary is about network derser so we are dealing with a
        // graph instance that should live in memory already - not expecting "big" stuff here.
        final List<Vertex> vertexList = IteratorUtils.list(value.vertices());
        final List<Edge> edgeList = IteratorUtils.list(value.edges());

        context.writeValue(vertexList.size(), buffer, false);

        for (Vertex v : vertexList) {
            writeVertex(buffer, context, v);
        }

        context.writeValue(edgeList.size(), buffer, false);

        for (Edge e : edgeList) {
            writeEdge(buffer, context, e);
        }
    }

    private void writeVertex(Buffer buffer, GraphBinaryWriter context, Vertex vertex) throws IOException {
        final List<VertexProperty<Object>> vertexProperties = IteratorUtils.list(vertex.properties());

        context.write(vertex.id(), buffer);
        // serializing label as list here for now according to GraphBinaryV4
        context.writeValue(Collections.singletonList(vertex.label()), buffer, false);
        context.writeValue(vertexProperties.size(), buffer, false);

        for (VertexProperty<Object> vp : vertexProperties) {
            context.write(vp.id(), buffer);
            context.writeValue(Collections.singletonList(vp.label()), buffer, false);
            context.write(vp.value(), buffer);

            // maintain the VertexProperty format we have with this empty parent.........
            context.write(null, buffer);

            // write those properties out using the standard Property serializer
            context.writeValue(IteratorUtils.list(vp.properties()), buffer, false);
        }
    }

    private void writeEdge(Buffer buffer, GraphBinaryWriter context, Edge edge) throws IOException {
        context.write(edge.id(), buffer);
        // serializing label as list here for now according to GraphBinaryV4
        context.writeValue(Collections.singletonList(edge.label()), buffer, false);

        context.write(edge.inVertex().id(), buffer);

        // vertex labels aren't needed but maintaining the Edge form that we have
        context.write(null, buffer);

        context.write(edge.outVertex().id(), buffer);

        // vertex labels aren't needed but maintaining the Edge form that we have
        context.write(null, buffer);

        // maintain the Edge format we have with this empty parent..................
        context.write(null, buffer);

        // write those properties out using the standard Property serializer
        context.writeValue(IteratorUtils.list(edge.properties()), buffer, false);
    }

    private static Map<String, List<VertexProperty>> indexedVertexProperties(final Vertex v) {
        final Map<String,List<VertexProperty>> index = new HashMap<>();
        v.properties().forEachRemaining(vp -> {
            if (!index.containsKey(vp.key())) {
                index.put(vp.key(), new ArrayList<>());
            }

            index.get(vp.key()).add(vp);
        });
        return index;
    }

    private static Method detectGraphOpenMethod() {
        final Class<?> graphClazz = detectTinkerGraph();

        // if no class then no method to lookup
        if (null == graphClazz) return null;

        try {
             return graphClazz.getMethod("open", Configuration.class);
        } catch (NoSuchMethodException nsme) {
            // famous last words - can't happen
            throw new IllegalStateException(nsme);
        }
    }

    private static Class<?> detectTinkerGraph() {
        // the java driver defaults to using TinkerGraph to deserialize Graph instances. if TinkerGraph isn't present
        // on the path, that's cool, users just won't be able to deserialize that
        try {
            return Class.forName("org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
        } catch (ClassNotFoundException cnfe) {
            return null;
        }
    }
}
