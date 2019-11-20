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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.script;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.CommonFileOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * {@link ScriptInputFormat} and {@code ScriptOutputFormat} take an arbitrary script and use that script to either
 * read or write Vertex objects, respectively. This can be considered the most general InputFormat/OutputFormat
 * possible in that Hadoop-Gremlin uses the user provided script for all reading/writing.
 * @see <a href="http://tinkerpop.apache.org/docs/current/reference/#script-io-format">Script I/O Format Reference Documentation</a>
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class ScriptOutputFormat extends CommonFileOutputFormat implements HadoopPoolsConfigurable {

    @Override
    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        return getRecordWriter(job, getDataOutputStream(job));
    }

    public RecordWriter<NullWritable, VertexWritable> getRecordWriter(final TaskAttemptContext job, final DataOutputStream outputStream) throws IOException, InterruptedException {
        return new ScriptRecordWriter(outputStream, job);
    }
}