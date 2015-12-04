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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
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
        try {
            final HadoopConfiguration hadoopConfiguration = new HadoopConfiguration(configuration);
            final BaseConfiguration newConfiguration = new BaseConfiguration();
            newConfiguration.copy(org.apache.tinkerpop.gremlin.hadoop.structure.io.InputOutputHelper.getOutputGraph(configuration, resultGraph, persist).configuration());
            if (resultGraph.equals(GraphComputer.ResultGraph.NEW) && hadoopConfiguration.containsKey(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD)) {
                newConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputRDDFormat.class.getCanonicalName());
                //newConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputRDDFormat.class.getCanonicalName());
                newConfiguration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, InputOutputHelper.getInputFormat((Class) Class.forName(hadoopConfiguration.getString(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD))).getCanonicalName());
                if (newConfiguration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "").endsWith("/" + Constants.HIDDEN_G)) {  // Spark RDDs are not namespaced the same as Hadoop
                    newConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, newConfiguration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION).replace("/" + Constants.HIDDEN_G, ""));
                }
            }
            return HadoopGraph.open(newConfiguration);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
