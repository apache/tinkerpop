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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InputOutputHelper {

    private static Map<Class<? extends InputFormat<NullWritable, VertexWritable>>, Class<? extends OutputFormat<NullWritable, VertexWritable>>> INPUT_TO_OUTPUT_CACHE = new ConcurrentHashMap<>();
    private static Map<Class<? extends OutputFormat<NullWritable, VertexWritable>>, Class<? extends InputFormat<NullWritable, VertexWritable>>> OUTPUT_TO_INPUT_CACHE = new ConcurrentHashMap<>();

    static {
        INPUT_TO_OUTPUT_CACHE.put(GryoInputFormat.class, GryoOutputFormat.class);
        INPUT_TO_OUTPUT_CACHE.put(GraphSONInputFormat.class, GraphSONOutputFormat.class);
        INPUT_TO_OUTPUT_CACHE.put(ScriptInputFormat.class, ScriptOutputFormat.class);
        //
        OUTPUT_TO_INPUT_CACHE.put(GryoOutputFormat.class, GryoInputFormat.class);
        OUTPUT_TO_INPUT_CACHE.put(GraphSONOutputFormat.class, GraphSONInputFormat.class);
        OUTPUT_TO_INPUT_CACHE.put(ScriptOutputFormat.class, ScriptInputFormat.class);
    }

    private InputOutputHelper() {

    }

    public static Class<? extends InputFormat> getInputFormat(final Class<? extends OutputFormat<NullWritable, VertexWritable>> outputFormat) {
        return OUTPUT_TO_INPUT_CACHE.get(outputFormat);
    }

    public static Class<? extends OutputFormat> getOutputFormat(final Class<? extends InputFormat<NullWritable, VertexWritable>> inputFormat) {
        return INPUT_TO_OUTPUT_CACHE.get(inputFormat);
    }

    public static void registerInputOutputPair(final Class<? extends InputFormat<NullWritable, VertexWritable>> inputFormat, final Class<? extends OutputFormat<NullWritable, VertexWritable>> outputFormat) {
        INPUT_TO_OUTPUT_CACHE.put(inputFormat, outputFormat);
        OUTPUT_TO_INPUT_CACHE.put(outputFormat, inputFormat);
    }

    public static HadoopGraph getOutputGraph(final Configuration configuration, final GraphComputer.ResultGraph resultGraph, final GraphComputer.Persist persist) {
        final HadoopConfiguration hadoopConfiguration = new HadoopConfiguration(configuration);
        final BaseConfiguration newConfiguration = new BaseConfiguration();
        newConfiguration.copy(hadoopConfiguration);
        if (resultGraph.equals(GraphComputer.ResultGraph.NEW)) {
            newConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, hadoopConfiguration.getOutputLocation());
            if (hadoopConfiguration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_WRITER))
                if (null != InputOutputHelper.getInputFormat(hadoopConfiguration.getGraphWriter()))
                    newConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, InputOutputHelper.getInputFormat(hadoopConfiguration.getGraphWriter()).getCanonicalName());
            newConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER_HAS_EDGES, persist.equals(GraphComputer.Persist.EDGES));
        }
        newConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, hadoopConfiguration.getOutputLocation() + "_");
        return HadoopGraph.open(newConfiguration);
    }
}
