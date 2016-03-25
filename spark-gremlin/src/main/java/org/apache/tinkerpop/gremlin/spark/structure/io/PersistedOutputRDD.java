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

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.PersistResultGraphAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PersistedOutputRDD implements OutputRDD, PersistResultGraphAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersistedOutputRDD.class);

    @Override
    public void writeGraphRDD(final Configuration configuration, final JavaPairRDD<Object, VertexWritable> graphRDD) {
        if (!configuration.getBoolean(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false))
            LOGGER.warn("The SparkContext should be persisted in order for the RDD to persist across jobs. To do so, set " + Constants.GREMLIN_SPARK_PERSIST_CONTEXT + " to true");
        if (!configuration.containsKey(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))
            throw new IllegalArgumentException("There is no provided " + Constants.GREMLIN_HADOOP_OUTPUT_LOCATION + " to write the persisted RDD to");
        SparkContextStorage.open(configuration).rm(configuration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));  // this might be bad cause it unpersists the job RDD
        // determine which storage level to persist the RDD as with MEMORY_ONLY being the default cache()
        final StorageLevel storageLevel = StorageLevel.fromString(configuration.getString(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_ONLY"));
        if (!configuration.getBoolean(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, true))
            graphRDD.mapValues(vertex -> {
                vertex.get().dropEdges(Direction.BOTH);
                return vertex;
            }).setName(Constants.getGraphLocation(configuration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))).persist(storageLevel);
        else
            graphRDD.setName(Constants.getGraphLocation(configuration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))).persist(storageLevel);
        Spark.refresh(); // necessary to do really fast so the Spark GC doesn't clear out the RDD
    }

    @Override
    public <K, V> Iterator<KeyValue<K, V>> writeMemoryRDD(final Configuration configuration, final String memoryKey, final JavaPairRDD<K, V> memoryRDD) {
        if (!configuration.getBoolean(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false))
            LOGGER.warn("The SparkContext should be persisted in order for the RDD to persist across jobs. To do so, set " + Constants.GREMLIN_SPARK_PERSIST_CONTEXT + " to true");
        if (!configuration.containsKey(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))
            throw new IllegalArgumentException("There is no provided " + Constants.GREMLIN_HADOOP_OUTPUT_LOCATION + " to write the persisted RDD to");
        final String memoryRDDName = Constants.getMemoryLocation(configuration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), memoryKey);
        Spark.removeRDD(memoryRDDName);
        memoryRDD.setName(memoryRDDName).persist(StorageLevel.fromString(configuration.getString(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_ONLY")));
        Spark.refresh(); // necessary to do really fast so the Spark GC doesn't clear out the RDD
        return IteratorUtils.map(memoryRDD.collect().iterator(), tuple -> new KeyValue<>(tuple._1(), tuple._2()));
    }

    @Override
    public boolean supportsResultGraphPersistCombination(final GraphComputer.ResultGraph resultGraph, final GraphComputer.Persist persist) {
        return persist.equals(GraphComputer.Persist.NOTHING) || resultGraph.equals(GraphComputer.ResultGraph.NEW);
    }
}
