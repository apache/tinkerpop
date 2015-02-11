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
package com.apache.tinkerpop.gremlin.hadoop.structure.io;

import com.apache.tinkerpop.gremlin.structure.Direction;
import com.apache.tinkerpop.gremlin.structure.Graph;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.apache.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import com.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexWritable<V extends Vertex> implements Writable {

    private Vertex vertex;
    private static final KryoWriter KRYO_WRITER = KryoWriter.build().create();
    private static final KryoReader KRYO_READER = KryoReader.build().create();

    public VertexWritable(final Vertex vertex) {
        this.vertex = vertex;
    }

    public void set(final Vertex vertex) {
        this.vertex = vertex;
    }

    public Vertex get() {
        return this.vertex;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[WritableUtils.readVInt(input)]);
        final Graph gLocal = TinkerGraph.open();
        this.vertex = KRYO_READER.readVertex(inputStream, Direction.BOTH,
                detachedVertex -> DetachedVertex.addTo(gLocal, detachedVertex),
                detachedEdge -> DetachedEdge.addTo(gLocal, detachedEdge));

    }

    @Override
    public void write(final DataOutput output) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        KRYO_WRITER.writeVertex(outputStream, this.vertex, Direction.BOTH);
        WritableUtils.writeVInt(output, outputStream.size());
        output.write(outputStream.toByteArray());
        outputStream.close();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof VertexWritable && ElementHelper.areEqual(this.vertex, ((VertexWritable) other).get());
    }

    @Override
    public int hashCode() {
        return this.vertex.hashCode();
    }

    @Override
    public String toString() {
        return this.vertex.toString();
    }
}
