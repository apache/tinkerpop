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
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.util.IoRegistryHelper;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphSONRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private GraphSONReader graphsonReader;
    private final VertexWritable vertexWritable = new VertexWritable();
    private final LineRecordReader lineRecordReader;
    private boolean hasEdges;

    public GraphSONRecordReader() {
        this.lineRecordReader = new LineRecordReader();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
        this.hasEdges = context.getConfiguration().getBoolean(Constants.GREMLIN_HADOOP_GRAPH_READER_HAS_EDGES, true);
        final GraphSONVersion graphSONVersion =
                GraphSONVersion.valueOf(context.getConfiguration().get(Constants.GREMLIN_HADOOP_GRAPHSON_VERSION, "V3_0"));
        final Mapper mapper = GraphSONMapper.build().
                version(graphSONVersion).
                typeInfo(TypeInfo.PARTIAL_TYPES).
                addDefaultXModule(true).
                addRegistries(IoRegistryHelper.createRegistries(
                        ConfUtil.makeApacheConfiguration(context.getConfiguration()))).create();
        this.graphsonReader = GraphSONReader.build().mapper(mapper).create();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        try (InputStream in = new ByteArrayInputStream(this.lineRecordReader.getCurrentValue().getBytes())) {
            this.vertexWritable.set(this.hasEdges ?
                    this.graphsonReader.readVertex(in, Attachable::get, Attachable::get, Direction.BOTH) :
                    this.graphsonReader.readVertex(in, Attachable::get));
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
        this.graphsonReader = null;
    }
}
