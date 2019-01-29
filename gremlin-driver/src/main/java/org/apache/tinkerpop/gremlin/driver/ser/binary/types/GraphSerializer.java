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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
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
    protected Graph readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {

        if (null == openMethod)
            throw new SerializationException("TinkerGraph is an optional dependency to gremlin-driver - if deserializing Graph instances it must be explicitly added as a dependency");

        final Configuration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.defaultVertexPropertyCardinality", "list");

        try {
            final Graph graph = (Graph) openMethod.invoke(null, conf);
            final int vertexCount = context.readValue(buffer, Integer.class, false);
            for (int ix = 0; ix < vertexCount; ix++) {
                final Vertex v = graph.addVertex(T.id, context.read(buffer), T.label, context.readValue(buffer, String.class, false));
                final int vertexPropertyCount = context.readValue(buffer, Integer.class, false);

                for (int iy = 0; iy < vertexPropertyCount; iy++) {
                    final Object id = context.read(buffer);
                    final String label = context.readValue(buffer, String.class, false);
                    final Object val = context.read(buffer);
                    context.read(buffer); // toss parent as it's always null
                    final VertexProperty<Object> vp = v.property(VertexProperty.Cardinality.list, label, val, T.id, id);

                    final List<Property> edgeProperties = context.readValue(buffer, ArrayList.class, false);
                    for (Property p : edgeProperties) {
                        vp.property(p.key(), p.value());
                    }
                }
            }

            final int edgeCount = context.readValue(buffer, Integer.class, false);
            for (int ix = 0; ix < edgeCount; ix++) {
                final Object id = context.read(buffer);
                final String label = context.readValue(buffer, String.class, false);
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
            throw new SerializationException(ex);
        }
    }

    @Override
    protected ByteBuf writeValue(final Graph value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        // this kinda looks scary memory-wise, but GraphBinary is about network derser so we are dealing with a
        // graph instance that should live in memory already - not expecting "big" stuff here.
        final List<Vertex> vertexList = IteratorUtils.list(value.vertices());
        final List<Edge> edgeList = IteratorUtils.list(value.edges());
        final CompositeByteBuf result = allocator.compositeBuffer(2 + edgeList.size() + vertexList.size());

        result.addComponent(true, context.writeValue(vertexList.size(), allocator, false));
        for (Vertex v : vertexList) {
            final List<VertexProperty<Object>> vertexProperties = IteratorUtils.list(v.properties());
            final CompositeByteBuf vbb = allocator.compositeBuffer(3 + vertexProperties.size());
            vbb.addComponent(true, context.write(v.id(), allocator));
            vbb.addComponent(true, context.writeValue(v.label(), allocator, false));

            vbb.addComponent(true, context.writeValue(vertexProperties.size(), allocator, false));
            for (VertexProperty<Object> vp : vertexProperties) {
                final CompositeByteBuf vpbb = allocator.compositeBuffer(5);
                vpbb.addComponent(true, context.write(vp.id(), allocator));
                vpbb.addComponent(true, context.writeValue(vp.label(), allocator, false));
                vpbb.addComponent(true, context.write(vp.value(), allocator));

                // maintain the VertexProperty format we have with this empty parent.........
                vpbb.addComponent(true, context.write(null, allocator));

                // write those properties out using the standard Property serializer
                vpbb.addComponent(true, context.writeValue(IteratorUtils.list(vp.properties()), allocator, false));

                vbb.addComponent(true, vpbb);
            }

            result.addComponent(true, vbb);
        }

        result.addComponent(true, context.writeValue(edgeList.size(), allocator, false));
        for (Edge e : edgeList) {
            final CompositeByteBuf ebb = allocator.compositeBuffer(8);
            ebb.addComponent(true, context.write(e.id(), allocator));
            ebb.addComponent(true, context.writeValue(e.label(), allocator, false));

            ebb.addComponent(true, context.write(e.inVertex().id(), allocator));

            // vertex labels aren't needed but maintaining the Edge form that we have
            ebb.addComponent(true, context.write(null, allocator));

            ebb.addComponent(true, context.write(e.outVertex().id(), allocator));

            // vertex labels aren't needed but maintaining the Edge form that we have
            ebb.addComponent(true, context.write(null, allocator));

            // maintain the Edge format we have with this empty parent..................
            ebb.addComponent(true, context.write(null, allocator));

            // write those properties out using the standard Property serializer
            ebb.addComponent(true, context.writeValue(IteratorUtils.list(e.properties()), allocator, false));

            result.addComponent(true, ebb);
        }

        return result;
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
