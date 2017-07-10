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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.util.IoRegistryHelper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphSONRecordWriter extends RecordWriter<NullWritable, VertexWritable> {
    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;
    private final DataOutputStream outputStream;
    private final boolean hasEdges;
    private final GraphSONWriter graphsonWriter;


    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }

    public GraphSONRecordWriter(final DataOutputStream outputStream, final Configuration configuration) {
        this.outputStream = outputStream;
        this.hasEdges = configuration.getBoolean(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, true);
        this.graphsonWriter = GraphSONWriter.build().mapper(
                GraphSONMapper.build().
                        version(GraphSONVersion.valueOf(configuration.get(Constants.GREMLIN_HADOOP_GRAPHSON_VERSION, "V3_0"))).
                        typeInfo(TypeInfo.PARTIAL_TYPES).
                        addRegistries(IoRegistryHelper.createRegistries(ConfUtil.makeApacheConfiguration(configuration))).create()).create();
    }

    @Override
    public void write(final NullWritable key, final VertexWritable vertex) throws IOException {
        if (null != vertex) {
            if (this.hasEdges) {
                graphsonWriter.writeVertex(this.outputStream, vertex.get(), Direction.BOTH);
                this.outputStream.write(NEWLINE);
            } else {
                graphsonWriter.writeVertex(this.outputStream, vertex.get());
                this.outputStream.write(NEWLINE);
            }
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.outputStream.close();
    }
}
