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
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
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
public final class SparkVertex implements Vertex, Serializable {

    private static GryoWriter GRYO_WRITER = GryoWriter.build().create();
    private static GryoReader GRYO_READER = GryoReader.build().create();

    // TODO: Wrapped vertex -- need VertexProgram in partition (broadcast variable?)

    private final Object vertexId;
    private transient TinkerVertex vertex;
    private byte[] vertexBytes;

    public SparkVertex(final TinkerVertex vertex) {
        this.vertex = vertex;
        this.vertexId = vertex.id();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        return this.vertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Object id() {
        return this.vertexId;
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
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return this.vertex.edges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return this.vertex.vertices(direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return this.vertex.properties(propertyKeys);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public int hashCode() {
        return this.vertexId.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this, other);
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

    public final void inflateVertex() {
        if (null != this.vertex)
            return;

        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(this.vertexBytes);
            final TinkerGraph tinkerGraph = TinkerGraph.open();
            GRYO_READER.readGraph(bis, tinkerGraph);
            bis.close();
            this.vertexBytes = null;
            this.vertex = (TinkerVertex) tinkerGraph.vertices(this.vertexId).next();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private final void deflateVertex() {
        if (null != this.vertexBytes)
            return;

        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GRYO_WRITER.writeGraph(bos, this.vertex.graph());
            bos.flush();
            bos.close();
            this.vertex = null;
            this.vertexBytes = bos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
