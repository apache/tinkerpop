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

package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Spark {

    private static SparkContext CONTEXT;
    private static final Map<String, RDD<?>> NAME_TO_RDD = new ConcurrentHashMap<>();

    private Spark() {
    }

    public static void create(final Configuration configuration) {
        final SparkConf sparkConf = new SparkConf();
        configuration.getKeys().forEachRemaining(key -> sparkConf.set(key, configuration.getProperty(key).toString()));
        sparkConf.setAppName("Spark-Gremlin Persisted Context Application");
        CONTEXT = SparkContext.getOrCreate(sparkConf);
    }

    public static void create(final String master) {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark-Gremlin Persisted Context Application");
        sparkConf.setMaster(master);
        CONTEXT = SparkContext.getOrCreate(sparkConf);
    }

    public static void create(final SparkContext sparkContext) {
        CONTEXT = sparkContext;
    }

    public static SparkContext getContext() {
        return CONTEXT;
    }

    public static void refresh() {
        if (null == CONTEXT)
            throw new IllegalStateException("The Spark context has not been created.");
        final Set<String> keepNames = new HashSet<>();
        for (final RDD<?> rdd : JavaConversions.asJavaIterable(CONTEXT.persistentRdds().values())) {
            if (null != rdd.name()) {
                keepNames.add(rdd.name());
                NAME_TO_RDD.put(rdd.name(), rdd);
            }
        }
        // remove all stale names in the NAME_TO_RDD map
        NAME_TO_RDD.keySet().stream().filter(key -> !keepNames.contains(key)).collect(Collectors.toList()).forEach(NAME_TO_RDD::remove);
    }

    public static RDD<?> getRDD(final String name) {
        Spark.refresh();
        if (!NAME_TO_RDD.containsKey(name))
            throw new IllegalArgumentException("The named RDD does not exist in the Spark context: " + name);
        return NAME_TO_RDD.get(name);
    }

    public static boolean hasRDD(final String name) {
        Spark.refresh();
        return NAME_TO_RDD.containsKey(name);
    }

    public static List<RDD<?>> getRDDs() {
        Spark.refresh();
        return new ArrayList<>(NAME_TO_RDD.values());
    }

    public static void removeRDD(final String name) {
        if (null == name)
            return;
        Spark.refresh();
        final RDD<?> rdd = NAME_TO_RDD.remove(name);
        if (null != rdd)
            rdd.unpersist(true);
    }

    public static void close() {
        if (null != CONTEXT)
            CONTEXT.stop();
        NAME_TO_RDD.clear();
    }
}
