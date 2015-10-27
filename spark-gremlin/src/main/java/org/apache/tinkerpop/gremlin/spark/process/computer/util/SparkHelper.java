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

package org.apache.tinkerpop.gremlin.spark.process.computer.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHelper {

    private SparkHelper() {

    }

    public static Optional<RDD<?>> getPersistedRDD(final JavaSparkContext sparkContext, final String rddName) {
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
}
