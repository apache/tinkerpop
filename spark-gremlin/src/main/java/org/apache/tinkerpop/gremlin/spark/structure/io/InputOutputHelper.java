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

package org.apache.tinkerpop.gremlin.spark.structure.io;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InputOutputHelper {

    private static Map<Class<? extends InputRDD>, Class<? extends OutputRDD>> INPUT_TO_OUTPUT_CACHE = new ConcurrentHashMap<>();
    private static Map<Class<? extends OutputRDD>, Class<? extends InputRDD>> OUTPUT_TO_INPUT_CACHE = new ConcurrentHashMap<>();

    static {
        INPUT_TO_OUTPUT_CACHE.put(PersistedInputRDD.class, PersistedOutputRDD.class);
        INPUT_TO_OUTPUT_CACHE.put(InputFormatRDD.class, OutputFormatRDD.class);
        //
        OUTPUT_TO_INPUT_CACHE.put(PersistedOutputRDD.class, PersistedInputRDD.class);
        OUTPUT_TO_INPUT_CACHE.put(OutputFormatRDD.class, InputFormatRDD.class);
    }

    private InputOutputHelper() {

    }

    public static Class<? extends InputRDD> getInputFormat(final Class<? extends OutputRDD> outputRDD) {
        return OUTPUT_TO_INPUT_CACHE.get(outputRDD);
    }

    public static Class<? extends OutputRDD> getOutputFormat(final Class<? extends InputRDD> inputRDD) {
        return INPUT_TO_OUTPUT_CACHE.get(inputRDD);
    }

    public static void registerInputOutputPair(final Class<? extends InputRDD> inputRDD, final Class<? extends OutputRDD> outputRDD) {
        INPUT_TO_OUTPUT_CACHE.put(inputRDD, outputRDD);
        OUTPUT_TO_INPUT_CACHE.put(outputRDD, inputRDD);
    }

    public static HadoopGraph getOutputGraph(final Configuration configuration, final GraphComputer.ResultGraph resultGraph, final GraphComputer.Persist persist) {
        final HadoopConfiguration hadoopConfiguration = new HadoopConfiguration(configuration);
        final BaseConfiguration newConfiguration = new BaseConfiguration();
        newConfiguration.copy(org.apache.tinkerpop.gremlin.hadoop.structure.io.InputOutputHelper.getOutputGraph(configuration, resultGraph, persist).configuration());
        if (resultGraph.equals(GraphComputer.ResultGraph.NEW) && hadoopConfiguration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_WRITER)) {
            if (null != InputOutputHelper.getInputFormat(hadoopConfiguration.getGraphWriter()))
                newConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, InputOutputHelper.getInputFormat(hadoopConfiguration.getGraphWriter()).getCanonicalName());
        }
        return HadoopGraph.open(newConfiguration);
    }
}
