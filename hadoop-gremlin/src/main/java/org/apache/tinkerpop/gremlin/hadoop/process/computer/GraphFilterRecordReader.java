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

package org.apache.tinkerpop.gremlin.hadoop.process.computer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilterRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private GraphFilter graphFilter = null;
    private RecordReader<NullWritable, VertexWritable> recordReader;

    public GraphFilterRecordReader() {
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final Configuration configuration = taskAttemptContext.getConfiguration();
        final InputFormat<NullWritable, VertexWritable> inputFormat = ReflectionUtils.newInstance(configuration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, InputFormat.class, InputFormat.class), configuration);
        if (!(inputFormat instanceof GraphFilterAware) && configuration.get(Constants.GREMLIN_HADOOP_GRAPH_FILTER, null) != null)
            this.graphFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(configuration), Constants.GREMLIN_HADOOP_GRAPH_FILTER);
        this.recordReader = inputFormat.createRecordReader(inputSplit, taskAttemptContext);
        this.recordReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (null == this.graphFilter) {
            return this.recordReader.nextKeyValue();
        } else {
            while (true) {
                if (this.recordReader.nextKeyValue()) {
                    final VertexWritable vertexWritable = this.recordReader.getCurrentValue();
                    final Optional<StarGraph.StarVertex> vertex = vertexWritable.get().applyGraphFilter(this.graphFilter);
                    if (vertex.isPresent()) {
                        vertexWritable.set(vertex.get());
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() throws IOException, InterruptedException {
        return this.recordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.recordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.recordReader.close();
    }
}
