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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PersistedInputRDD implements InputRDD {

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        if (!configuration.containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION))
            throw new IllegalArgumentException("There is no provided " + Constants.GREMLIN_HADOOP_INPUT_LOCATION + " to read the persisted RDD from");
        Spark.create(sparkContext.sc());
        final Optional<String> graphLocation = Constants.getSearchGraphLocation(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), SparkContextStorage.open());
        return graphLocation.isPresent() ? JavaPairRDD.fromJavaRDD((JavaRDD) Spark.getRDD(graphLocation.get()).toJavaRDD()) : JavaPairRDD.fromJavaRDD(sparkContext.emptyRDD());
    }

    @Override
    public <K, V> JavaPairRDD<K, V> readMemoryRDD(final Configuration configuration, final String memoryKey, final JavaSparkContext sparkContext) {
        if (!configuration.containsKey(Constants.GREMLIN_HADOOP_INPUT_LOCATION))
            throw new IllegalArgumentException("There is no provided " + Constants.GREMLIN_HADOOP_INPUT_LOCATION + " to read the persisted RDD from");
        return JavaPairRDD.fromJavaRDD((JavaRDD) Spark.getRDD(Constants.getMemoryLocation(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), memoryKey)).toJavaRDD());
    }
}
