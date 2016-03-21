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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoRecordWriter extends RecordWriter<NullWritable, VertexWritable> {

    private final DataOutputStream outputStream;
    private final boolean hasEdges;
    private GryoWriter gryoWriter;

    public GryoRecordWriter(final DataOutputStream outputStream, final Configuration configuration) {
        this.outputStream = outputStream;
        this.hasEdges = configuration.getBoolean(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, true);
        HadoopPools.initialize(configuration);
        this.gryoWriter = HadoopPools.getGryoPool().takeWriter();
    }

    @Override
    public void write(final NullWritable key, final VertexWritable vertex) throws IOException {
        if (null != vertex) {
            if (this.hasEdges)
                gryoWriter.writeVertex(this.outputStream, vertex.get(), Direction.BOTH);
            else
                gryoWriter.writeVertex(this.outputStream, vertex.get());
        }
    }

    @Override
    public synchronized void close(final TaskAttemptContext context) throws IOException {
        this.outputStream.close();
        if (null != this.gryoWriter) {
            HadoopPools.getGryoPool().offerWriter(this.gryoWriter);
            this.gryoWriter = null;
        }
    }
}
