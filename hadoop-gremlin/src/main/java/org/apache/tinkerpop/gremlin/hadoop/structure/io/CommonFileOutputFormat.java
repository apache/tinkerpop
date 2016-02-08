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
package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.PersistResultGraphAware;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CommonFileOutputFormat extends FileOutputFormat<NullWritable, VertexWritable> implements PersistResultGraphAware {

    protected DataOutputStream getDataOutputStream(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            final Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = super.getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            return new DataOutputStream(fs.create(file, false));
        } else {
            return new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
        }
    }

    @Override
    public boolean supportsResultGraphPersistCombination(final GraphComputer.ResultGraph resultGraph, final GraphComputer.Persist persist) {
        return persist.equals(GraphComputer.Persist.NOTHING) || resultGraph.equals(GraphComputer.ResultGraph.NEW);
    }
}
