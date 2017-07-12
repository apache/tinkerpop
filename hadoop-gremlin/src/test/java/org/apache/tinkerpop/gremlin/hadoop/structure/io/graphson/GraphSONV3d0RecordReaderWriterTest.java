/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.RecordReaderWriterTest;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONV3d0RecordReaderWriterTest extends RecordReaderWriterTest {

    public GraphSONV3d0RecordReaderWriterTest() {
        // should be default
        // super.configuration.set(Constants.GREMLIN_HADOOP_GRAPHSON_VERSION, GraphSONVersion.V3_0.name());
    }

    @Override
    protected String getInputFilename() {
        return "grateful-dead-v3d0.json";
    }

    @Override
    protected Class<? extends InputFormat<NullWritable, VertexWritable>> getInputFormat() {
        return GraphSONInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat<NullWritable, VertexWritable>> getOutputFormat() {
        return GraphSONOutputFormat.class;
    }
}

