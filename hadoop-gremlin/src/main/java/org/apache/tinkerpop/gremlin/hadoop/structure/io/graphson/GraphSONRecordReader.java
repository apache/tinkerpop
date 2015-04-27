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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private final GraphSONReader graphsonReader = GraphSONReader.build().create();
    private final VertexWritable vertexWritable = new VertexWritable();
    private final LineRecordReader lineRecordReader;
    private boolean hasEdges;

    public GraphSONRecordReader() {
        this.lineRecordReader = new LineRecordReader();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
        this.hasEdges = context.getConfiguration().getBoolean(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT_HAS_EDGES, true);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        final StarGraph starGraph = StarGraph.open();
        final Function<Attachable<Vertex>, Vertex> vertexMaker = attachableVertex -> attachableVertex.attach(Attachable.Method.create(starGraph));
        final Function<Attachable<Edge>, Edge> edgeMaker = attachableEdge -> attachableEdge.attach(Attachable.Method.create(starGraph));
        try (InputStream in = new ByteArrayInputStream(this.lineRecordReader.getCurrentValue().getBytes())) {
            this.vertexWritable.set(this.hasEdges ?
                    this.graphsonReader.readVertex(in, vertexMaker, edgeMaker, Direction.BOTH) :
                    this.graphsonReader.readVertex(in, vertexMaker));
            return true;
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return this.vertexWritable;
    }

    @Override
    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}
