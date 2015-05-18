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
package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexWritable implements Writable, Serializable {

    private StarGraph.StarVertex vertex;

    public VertexWritable() {

    }

    public VertexWritable(final Vertex vertex) {
        this.set(vertex);
    }

    public void set(final Vertex vertex) {
        this.vertex = vertex instanceof StarGraph.StarVertex ?
                (StarGraph.StarVertex) vertex :
                StarGraph.of(vertex).getStarVertex();
    }

    public StarGraph.StarVertex get() {
        return this.vertex;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        try {
            this.vertex = null;
            this.vertex = HadoopPools.getGryoPool().doWithReader(gryoReader -> {
                try {
                    final ByteArrayInputStream inputStream = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
                    return gryoReader.readObject(inputStream, StarGraph.class).getStarVertex(); // read the star graph
                } catch (final IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            });
        } catch (final IllegalStateException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException) e.getCause();
            else
                throw e;
        }
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        try {
            HadoopPools.getGryoPool().doWithWriter(gryoWriter -> {
                try {
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    gryoWriter.writeObject(outputStream, this.vertex.graph()); // write the star graph
                    WritableUtils.writeCompressedByteArray(output, outputStream.toByteArray());
                } catch (final IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            });
        } catch (final IllegalStateException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException) e.getCause();
            else
                throw e;
        }
    }

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.write(outputStream);
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        this.readFields(inputStream);
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
