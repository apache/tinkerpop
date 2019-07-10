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

import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import scala.Tuple2;

/**
 * An InputRDD is used to read data from the underlying graph system and yield the respective adjacency list.
 * Note that {@link InputFormatRDD} is a type of InputRDD that simply uses the specified {@link org.apache.hadoop.mapreduce.InputFormat} to generate the respective graphRDD.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface InputRDD {

    /**
     * Read the graphRDD from the underlying graph system.
     *
     * @param configuration the configuration for the {@link org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer}
     * @param sparkContext  the Spark context with the requisite methods for generating a {@link JavaPairRDD}
     * @return an adjacency list representation of the underlying graph system.
     */
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext);

    /**
     * Read a memoryRDD from the storage location.
     * The default implementation returns an empty RDD.
     *
     * @param configuration the configuration for the {@link org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer}
     * @param memoryKey     the memory key of the memoryRDD
     * @param sparkContext  the Spark context with the requisite methods for generating a {@link JavaPairRDD}
     * @param <K>           the key class of the memoryRDD
     * @param <V>           the value class of the memoryRDD
     * @return the memoryRDD with respective key/value pairs.
     */
    public default <K, V> JavaPairRDD<K, V> readMemoryRDD(final Configuration configuration, final String memoryKey, final JavaSparkContext sparkContext) {
        return sparkContext.<Tuple2<K, V>>emptyRDD().mapToPair(t -> t);
    }
}
