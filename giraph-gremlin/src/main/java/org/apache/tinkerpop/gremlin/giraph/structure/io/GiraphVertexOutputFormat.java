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

import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphVertexOutputFormat extends VertexOutputFormat {

    private OutputFormat<NullWritable, VertexWritable> hadoopGraphOutputFormat;

    @Override
    public VertexWriter createVertexWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        return new GiraphVertexWriter(this.hadoopGraphOutputFormat);
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        this.hadoopGraphOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        return this.hadoopGraphOutputFormat.getOutputCommitter(context);
    }

    private final void constructor(final Configuration configuration) {
        if (null == this.hadoopGraphOutputFormat) {
            this.hadoopGraphOutputFormat = ReflectionUtils.newInstance(configuration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class), configuration);
        }
    }
}