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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkVertex implements Vertex, Vertex.Iterators, Serializable {

    private static KryoWriter KRYO_WRITER = KryoWriter.build().create();
    private static KryoReader KRYO_READER = KryoReader.build().create();
    private static final String VERTEX_ID = Graph.Hidden.hide("giraph.gremlin.vertexId");

    private transient TinkerVertex vertex;
    private byte[] vertexBytes;

    public SparkVertex(final TinkerVertex vertex) {
        this.vertex = vertex;
        this.vertex.graph().variables().set(VERTEX_ID, this.vertex.id());
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        return this.vertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Object id() {
        return this.vertex.id();
    }

    @Override
    public String label() {
        return this.vertex.label();
    }

    @Override
    public Graph graph() {
        return this.vertex.graph();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.vertex.property(key, value);
    }

    @Override
    public void remove() {
        this.vertex.remove();
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return this.vertex.iterators().edgeIterator(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        return this.vertex.iterators().vertexIterator(direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return this.vertex.iterators().propertyIterator(propertyKeys);
    }

    ///////////////////////////////

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.deflateVertex();
        outputStream.defaultWriteObject();
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        this.inflateVertex();
    }

    private final void inflateVertex() {
        if (null != this.vertex)
            return;

        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(this.vertexBytes);
            final TinkerGraph tinkerGraph = TinkerGraph.open();
            KRYO_READER.readGraph(bis, tinkerGraph);
            bis.close();
            this.vertexBytes = null;
            this.vertex = (TinkerVertex) tinkerGraph.iterators().vertexIterator(tinkerGraph.variables().get(VERTEX_ID).get()).next();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private final void deflateVertex() {
        if (null != this.vertexBytes)
            return;

        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            KRYO_WRITER.writeGraph(bos, this.vertex.graph());
            bos.flush();
            bos.close();
            this.vertex = null;
            this.vertexBytes = bos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
