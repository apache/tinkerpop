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
package org.apache.tinkerpop.gremlin.hadoop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Storage;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Constants {

    private Constants() {
    }

    /**
     * A key mapped to non-null, not empty UNIX-formatted file system path
     * @see #getGraphLocation(String)
     * @see #getMemoryLocation(String, String)
     */
    public static final String GREMLIN_HADOOP_INPUT_LOCATION = "gremlin.hadoop.inputLocation";

    /**
     * A key mapped to non-null, not empty UNIX-formatted file system path to store the graph, memory
     * and other files and directories, specific for Hadoop.
     * @see #getGraphLocation(String)
     * @see #getMemoryLocation(String, String)
     */
    public static final String GREMLIN_HADOOP_OUTPUT_LOCATION = "gremlin.hadoop.outputLocation";

    public static final String GREMLIN_HADOOP_GRAPH_READER = "gremlin.hadoop.graphReader";
    public static final String GREMLIN_HADOOP_GRAPH_WRITER = "gremlin.hadoop.graphWriter";
    public static final String GREMLIN_HADOOP_GRAPH_READER_HAS_EDGES = "gremlin.hadoop.graphReader.hasEdges";
    public static final String GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES = "gremlin.hadoop.graphWriter.hasEdges";
    public static final String GREMLIN_HADOOP_GRAPH_FILTER = "gremlin.hadoop.graphFilter";
    public static final String GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER = "gremlin.hadoop.defaultGraphComputer";
    public static final String GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR = "gremlin.hadoop.vertexProgramInterceptor";
    public static final String GREMLIN_HADOOP_GRAPHSON_VERSION = "gremlin.hadoop.graphSONVersion";

    public static final String GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE = "gremlin.hadoop.jarsInDistributedCache";
    public static final String HIDDEN_G = Graph.Hidden.hide("g");
    public static final String GREMLIN_HADOOP_JOB_PREFIX = "HadoopGremlin: ";
    // public static final String GREMLIN_HADOOP_MAP_REDUCE_JOB_PREFIX = "HadoopGremlin(MapReduce): ";
    public static final String GREMLIN_HADOOP_SPARK_JOB_PREFIX = "HadoopGremlin(Spark): ";
    public static final String HADOOP_GREMLIN_LIBS = "HADOOP_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String HIDDEN_ITERATION = Graph.Hidden.hide("gremlin.hadoop.iteration");
    public static final String GREMLIN_HADOOP_MAP_REDUCE_CLASS = "gremlin.hadoop.mapReduceClass";

    public static final String MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR = "mapreduce.input.fileinputformat.inputdir";

    // spark based constants
    public static final String GREMLIN_SPARK_PERSIST_CONTEXT = "gremlin.spark.persistContext";
    public static final String GREMLIN_SPARK_GRAPH_STORAGE_LEVEL = "gremlin.spark.graphStorageLevel";
    public static final String GREMLIN_SPARK_PERSIST_STORAGE_LEVEL = "gremlin.spark.persistStorageLevel";
    public static final String GREMLIN_SPARK_SKIP_PARTITIONER = "gremlin.spark.skipPartitioner"; // don't partition the loadedGraphRDD
    public static final String GREMLIN_SPARK_SKIP_GRAPH_CACHE = "gremlin.spark.skipGraphCache";  // don't cache the loadedGraphRDD (ignores graphStorageLevel)
    public static final String SPARK_SERIALIZER = "spark.serializer";
    public static final String SPARK_KRYO_REGISTRATOR = "spark.kryo.registrator";
    public static final String SPARK_KRYO_REGISTRATION_REQUIRED = "spark.kryo.registrationRequired";

    /**
     * @param location not null, not empty UNIX-formatted file system path
     * @return not empty UNIX-formatted file system path to the graph in the location, still compatible with Windows and java.io.File
     */
    public static String getGraphLocation(final String location) {
        return location.endsWith("/") ? location + Constants.HIDDEN_G : location + "/" + Constants.HIDDEN_G;
    }

    /**
     * @param location not null, not empty UNIX-formatted file system path
     * @param memoryKey not null, not empty
     * @return not empty UNIX-formatted file system path to the memory in the location, still compatible with Windows and java.io.File
     */
    public static String getMemoryLocation(final String location, final String memoryKey) {
        return location.endsWith("/") ? location + memoryKey : location + "/" + memoryKey;
    }

    public static Optional<String> getSearchGraphLocation(final String location, final Storage storage) {
        if (storage.exists(getGraphLocation(location)))
            return Optional.of(getGraphLocation(location));
        else if (storage.exists(location))
            return Optional.of(location);
        else
            return Optional.empty();
    }
}
