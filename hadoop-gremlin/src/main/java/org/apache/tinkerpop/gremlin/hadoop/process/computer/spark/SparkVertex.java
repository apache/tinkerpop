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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkVertex implements Vertex, Vertex.Iterators, Serializable {

    private static KryoWriter KRYO_WRITER = KryoWriter.build().create();
    private static KryoReader KRYO_READER = KryoReader.build().create();
    private static final String VERTEX_ID = Graph.Hidden.hide("giraph.gremlin.vertexId");

    private transient TinkerVertex vertex;
    private byte[] serializedForm;

    public SparkVertex(final TinkerVertex vertex) {
        this.vertex = vertex;
        this.vertex.graph().variables().set(VERTEX_ID, this.vertex.id());
        this.deflateVertex();
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        inflateVertex();
        return this.vertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Object id() {
        inflateVertex();
        return this.vertex.id();
    }

    @Override
    public String label() {
        inflateVertex();
        return this.vertex.label();
    }

    @Override
    public Graph graph() {
        inflateVertex();
        return this.vertex.graph();
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        inflateVertex();
        return this.vertex.property(key, value);
    }

    @Override
    public void remove() {
        inflateVertex();
        this.vertex.remove();
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(Direction direction, String... edgeLabels) {
        inflateVertex();
        return this.vertex.iterators().edgeIterator(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, String... edgeLabels) {
        inflateVertex();
        return this.vertex.iterators().vertexIterator(direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... propertyKeys) {
        inflateVertex();
        return this.vertex.iterators().propertyIterator(propertyKeys);
    }

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.inflateVertex();
        this.deflateVertex();
        outputStream.defaultWriteObject();
    }

    private final void inflateVertex() {
        if (null != this.vertex)
            return;

        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(this.serializedForm);
            final TinkerGraph tinkerGraph = TinkerGraph.open();
            KRYO_READER.readGraph(bis, tinkerGraph);
            bis.close();
            this.vertex = (TinkerVertex) tinkerGraph.iterators().vertexIterator(tinkerGraph.variables().get(VERTEX_ID).get()).next();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private final void deflateVertex() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            KRYO_WRITER.writeGraph(bos, this.vertex.graph());
            bos.flush();
            bos.close();
            this.serializedForm = bos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
