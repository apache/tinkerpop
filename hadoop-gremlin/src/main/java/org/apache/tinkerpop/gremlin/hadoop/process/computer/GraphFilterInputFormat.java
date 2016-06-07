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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;

import java.io.IOException;
import java.util.List;

/**
 * GraphFilterInputFormat is a utility {@link InputFormat} that is useful if the underlying InputFormat is not {@link GraphFilterAware}.
 * If the underlying InputFormat is GraphFilterAware, then GraphFilterInputFormat acts as an identity mapping.
 * If the underlying InputFormat is not GraphFilterAware, then GraphFilterInputFormat will apply the respective {@link GraphFilter}
 * and prune the loaded source graph data accordingly.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilterInputFormat extends InputFormat<NullWritable, VertexWritable> implements GraphFilterAware {

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        final Configuration configuration = jobContext.getConfiguration();
        return ReflectionUtils.newInstance(configuration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, InputFormat.class, InputFormat.class), configuration).getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new GraphFilterRecordReader();
    }

    @Override
    public void setGraphFilter(final GraphFilter graphFilter) {
        // do nothing -- loaded via configuration
    }
}
