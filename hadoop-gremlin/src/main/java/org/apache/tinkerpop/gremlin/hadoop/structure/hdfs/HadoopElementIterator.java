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
package org.apache.tinkerpop.gremlin.hadoop.structure.hdfs;

import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopElementIterator<E extends Element> implements Iterator<E> {

    // TODO: Generalize so it works for more than just FileFormats.

    protected final HadoopGraph graph;
    protected final Queue<RecordReader<NullWritable, VertexWritable>> readers = new LinkedList<>();

    public HadoopElementIterator(final HadoopGraph graph, final InputFormat<NullWritable, VertexWritable> inputFormat, final Path path) throws IOException, InterruptedException {
        this.graph = graph;
        final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
        for (final FileStatus status : FileSystem.get(configuration).listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(inputFormat.createRecordReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
        }
    }

    public HadoopElementIterator(final HadoopGraph graph) throws IOException {
        try {
            this.graph = graph;
            if (this.graph.configuration().containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION)) {
                final Configuration configuration = ConfUtil.makeHadoopConfiguration(this.graph.configuration());
                final InputFormat<NullWritable, VertexWritable> inputFormat = this.graph.configuration().getGraphInputFormat().getConstructor().newInstance();
                for (final FileStatus status : FileSystem.get(configuration).listStatus(new Path(graph.configuration().getInputLocation()), HiddenFileFilter.instance())) {
                    this.readers.add(inputFormat.createRecordReader(new FileSplit(status.getPath(), 0, Integer.MAX_VALUE, new String[]{}), new TaskAttemptContext(configuration, new TaskAttemptID())));
                }
            }
        } catch (ClosedByInterruptException cbie) {
            // this is the exception that seems to rise when a Traversal interrupt occurs.  Convert it to the
            // common exception expected.
            throw new TraversalInterruptedException(cbie);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
