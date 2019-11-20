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
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.Memory;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface OutputRDD {

    /**
     * Write the graphRDD to an output location. The {@link Configuration} maintains the specified location via {@link Constants#GREMLIN_HADOOP_OUTPUT_LOCATION}.
     *
     * @param configuration the configuration of the Spark job
     * @param graphRDD      the graphRDD to output
     */
    public void writeGraphRDD(final Configuration configuration, final JavaPairRDD<Object, VertexWritable> graphRDD);

    /**
     * Write the sideEffect memoryRDD to an output location. The {@link Configuration} maintains the specified location via {@link Constants#GREMLIN_HADOOP_OUTPUT_LOCATION}.
     * The default implementation returns an empty iterator.
     *
     * @param configuration the configuration of the Spark job
     * @param memoryKey     the memory key of the memoryRDD
     * @param memoryRDD     the memoryRDD
     * @param <K>           the key class of the RDD
     * @param <V>           the value class of the RDD
     * @return the {@link KeyValue} iterator to store in the final resultant {@link Memory}.
     */
    public default <K, V> Iterator<KeyValue<K, V>> writeMemoryRDD(final Configuration configuration, final String memoryKey, final JavaPairRDD<K, V> memoryRDD) {
        return Collections.emptyIterator();
    }
}
