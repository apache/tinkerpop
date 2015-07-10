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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.RecordReaderWriterTest;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;

/**
 * Created by Edi Bice on 6/18/2015.
 */
public class GraphSONLegacyRecordReaderTest extends RecordReaderWriterTest {

    @Override
    protected String getInputFilename() {
        return "grateful-dead-tp2adj.json";
        //return "tinkerpop2adj-classic.json";
    }

    @Override
    protected Class<? extends InputFormat<NullWritable, VertexWritable>> getInputFormat() {
        return GraphSONLegacyInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat<NullWritable, VertexWritable>> getOutputFormat() {
        return GraphSONOutputFormat.class;
    }
}