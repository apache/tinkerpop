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
package org.apache.tinkerpop.gremlin.giraph.structure.io;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphVertexWriter extends VertexWriter {
    private final OutputFormat<NullWritable, VertexWritable> outputFormat;
    private RecordWriter<NullWritable, VertexWritable> recordWriter;

    public GiraphVertexWriter(final OutputFormat<NullWritable, VertexWritable> outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public void initialize(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordWriter = this.outputFormat.getRecordWriter(context);
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordWriter.close(context);
    }

    @Override
    public void writeVertex(final Vertex vertex) throws IOException, InterruptedException {
        this.recordWriter.write(NullWritable.get(), ((GiraphVertex) vertex).getValue());
    }
}
