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
package com.apache.tinkerpop.gremlin.hadoop;

import com.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Constants {
    public static final String GREMLIN_HADOOP_INPUT_LOCATION = "gremlin.hadoop.inputLocation";
    public static final String GREMLIN_HADOOP_OUTPUT_LOCATION = "gremlin.hadoop.outputLocation";
    public static final String GREMLIN_HADOOP_GRAPH_INPUT_FORMAT = "gremlin.hadoop.graphInputFormat";
    public static final String GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT = "gremlin.hadoop.graphOutputFormat";
    public static final String GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT = "gremlin.hadoop.memoryOutputFormat";

    public static final String GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE = "gremlin.hadoop.jarsInDistributedCache";
    public static final String SYSTEM_G = Graph.Hidden.hide("g");
    public static final String GREMLIN_HADOOP_JOB_PREFIX = "HadoopGremlin: ";
    public static final String GREMLIN_HADOOP_GIRAPH_JOB_PREFIX = "HadoopGremlin(Giraph): ";
    public static final String GREMLIN_HADOOP_MAP_REDUCE_JOB_PREFIX = "HadoopGremlin(MapReduce): ";
    public static final String HADOOP_GREMLIN_LIBS = "HADOOP_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_HADOOP_DERIVE_MEMORY = "gremlin.hadoop.deriveMemory";
    public static final String SYSTEM_MEMORY = Graph.Hidden.hide("memory");
    public static final String SYSTEM_RUNTIME = Graph.Hidden.hide("gremlin.hadoop.runtime");
    public static final String SYSTEM_ITERATION = Graph.Hidden.hide("gremlin.hadoop.iteration");
    public static final String GREMLIN_HADOOP_MEMORY_KEYS = "gremlin.hadoop.memoryKeys";
    public static final String GREMLIN_HADOOP_MAP_REDUCE_CLASS = "gremlin.hadoop.mapReduceClass";
    public static final String GREMLIN_HADOOP_HALT = "gremlin.hadoop.halt";
    public static final String MAP_MEMORY = "gremlin.hadoop.mapMemory";
}
