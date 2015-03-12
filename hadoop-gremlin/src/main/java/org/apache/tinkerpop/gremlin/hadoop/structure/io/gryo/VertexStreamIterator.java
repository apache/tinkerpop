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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStreamIterator implements Iterator<VertexWritable> {

    // this is VertexTerminator's long terminal 4185403236219066774L as an array of positive int's
    private static final int[] TERMINATOR = new int[]{58, 21, 138, 17, 112, 155, 153, 150};

    private final InputStream inputStream;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final GryoReader gryoReader = GryoReader.build().create();

    private int currentByte;
    private long currentTotalLength = 0;
    private Vertex currentVertex;
    private final VertexWritable vertexWritable = new VertexWritable();
    private final long maxLength;

    public VertexStreamIterator(final InputStream inputStream, final long maxLength) {
        this.inputStream = inputStream;
        this.maxLength = maxLength;
    }

    public float getProgress() {
        if (0 == this.currentTotalLength || 0 == this.maxLength)
            return 0.0f;
        else if (this.currentTotalLength >= this.maxLength || this.maxLength == Long.MAX_VALUE)
            return 1.0f;
        else
            return (float) this.currentTotalLength / (float) this.maxLength;

    }

    @Override
    public boolean hasNext() {
        if (this.currentTotalLength >= this.maxLength) // gone beyond the split boundary
            return false;
        if (null != this.currentVertex)
            return true;
        else if (-1 == this.currentByte)
            return false;
        else {
            try {
                this.currentVertex = advanceToNextVertex();
                return null != this.currentVertex;
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    @Override
    public VertexWritable next() {
        try {
            if (null == this.currentVertex) {
                if (this.hasNext()) {
                    this.vertexWritable.set(this.currentVertex);
                    return this.vertexWritable;
                } else
                    throw new IllegalStateException("There are no more vertices in this split");
            } else {
                this.vertexWritable.set(this.currentVertex);
                return this.vertexWritable;
            }
        } finally {
            this.currentVertex = null;
            this.output.reset();
        }
    }

    private final Vertex advanceToNextVertex() throws IOException {
        long currentVertexLength = 0;
        int terminatorLocation = 0;
        while (true) {
            this.currentByte = this.inputStream.read();
            if (-1 == this.currentByte) {
                if (currentVertexLength > 0)
                    throw new IllegalStateException("Remainder of stream exhausted without matching a vertex");
                else
                    return null;
            }
            this.currentTotalLength++;
            currentVertexLength++;
            this.output.write(this.currentByte);

            if (this.currentByte == TERMINATOR[terminatorLocation])
                terminatorLocation++;
            else
                terminatorLocation = 0;

            if (terminatorLocation >= TERMINATOR.length) {
                final Graph gLocal = TinkerGraph.open();
                final Function<DetachedVertex, Vertex> vertexMaker = detachedVertex -> DetachedVertex.addTo(gLocal, detachedVertex);
                final Function<DetachedEdge, Edge> edgeMaker = detachedEdge -> DetachedEdge.addTo(gLocal, detachedEdge);
                try (InputStream in = new ByteArrayInputStream(this.output.toByteArray())) {
                    return this.gryoReader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
                }
            }
        }
    }
}
