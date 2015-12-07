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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PersistedInputRDD implements InputRDD {

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        Spark.create(sparkContext.sc());
        final String inputRDDName = configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION, null);
        if (null == inputRDDName)
            throw new IllegalArgumentException(PersistedInputRDD.class.getSimpleName() + " requires " + Constants.GREMLIN_HADOOP_INPUT_LOCATION + " in order to retrieve the named graphRDD from the SparkContext");
        return JavaPairRDD.fromJavaRDD((JavaRDD) Spark.getRDD(inputRDDName).toJavaRDD());
    }

    /*public static Optional<RDD<?>> getPersistedRDD(final JavaSparkContext sparkContext, final String rddName) {
        final Iterator<Tuple2<Object, RDD<?>>> iterator = JavaSparkContext.toSparkContext(sparkContext).
                getPersistentRDDs().
                toList().iterator();
        while (iterator.hasNext()) {
            final Tuple2<Object, RDD<?>> tuple2 = iterator.next();
            if (tuple2._2().toString().contains(rddName))
                return Optional.of(tuple2._2());
        }
        return Optional.empty();
    }

    public static void removePersistedRDD(final JavaSparkContext sparkContext, final String rddName) {
        if (null == rddName)
            return;
        final List<Object> matchingRDDs = new ArrayList<>();
        final Iterator<Tuple2<Object, RDD<?>>> iterator = JavaSparkContext.toSparkContext(sparkContext).
                getPersistentRDDs().
                toList().iterator();
        while (iterator.hasNext()) {
            final Tuple2<Object, RDD<?>> tuple2 = iterator.next();
            if (tuple2._2().toString().contains(rddName))
                matchingRDDs.add(tuple2._1());
        }
        for (final Object rddId : matchingRDDs) {
            JavaSparkContext.toSparkContext(sparkContext).persistentRdds().remove(rddId).get().unpersist(false);
        }
    }*/
}
