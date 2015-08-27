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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

import java.io.IOException;

/**
 * Created by Edi Bice on 6/18/2015.
 */
public final class GraphSONLegacyInputFormat extends FileInputFormat<NullWritable, VertexWritable> implements HadoopPoolsConfigurable {

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<NullWritable, VertexWritable> reader = new GraphSONLegacyRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }
}
