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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CommonFileInputFormat extends FileInputFormat<NullWritable, VertexWritable> implements HadoopPoolsConfigurable, GraphFilterAware {

    protected Traversal.Admin<Vertex, Vertex> vertexFilter = null;
    protected Traversal.Admin<Edge, Edge> edgeFilter = null;
    private boolean filtersLoader = false;

    protected void loadVertexAndEdgeFilters(final TaskAttemptContext context) {
        if (!this.filtersLoader) {
            if (context.getConfiguration().get(Constants.GREMLIN_HADOOP_VERTEX_FILTER, null) != null)
                this.vertexFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(context.getConfiguration()), Constants.GREMLIN_HADOOP_VERTEX_FILTER);
            if (context.getConfiguration().get(Constants.GREMLIN_HADOOP_EDGE_FILTER, null) != null)
                this.edgeFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(context.getConfiguration()), Constants.GREMLIN_HADOOP_EDGE_FILTER);
            this.filtersLoader = true;
        }
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

    @Override
    public void setVertexFilter(final Traversal<Vertex, Vertex> vertexFilter) {
        // do nothing. loaded through configuration.
    }

    @Override
    public void setEdgeFilter(final Traversal<Edge, Edge> edgeFilter) {
        // do nothing. loaded through configuration.
    }
}
