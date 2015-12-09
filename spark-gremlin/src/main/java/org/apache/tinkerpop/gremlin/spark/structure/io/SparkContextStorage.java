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
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkContextStorage implements Storage {

    private SparkContextStorage() {

    }

    public static SparkContextStorage open() {
        return new SparkContextStorage();
    }

    public static SparkContextStorage open(final String master) {
        Spark.create(master);
        return new SparkContextStorage();
    }

    public static SparkContextStorage open(final Configuration configuration) {
        Spark.create(configuration);
        return new SparkContextStorage();
    }

    public static SparkContextStorage open(final SparkContext sparkContext) {
        Spark.create(sparkContext);
        return new SparkContextStorage();
    }


    @Override
    public List<String> ls() {
        return ls("*");
    }

    @Override
    public List<String> ls(final String location) {
        final List<String> rdds = new ArrayList<>();
        final String wildCardLocation = location.replace(".", "\\.").replace("*", ".*");
        for (final RDD<?> rdd : Spark.getRDDs()) {
            if (rdd.name().matches(wildCardLocation))
                rdds.add(rdd.name() + " [" + rdd.getStorageLevel().description() + "]");
        }
        return rdds;
    }

    @Override
    public boolean mkdir(final String location) {
        throw new UnsupportedOperationException("This operation does not make sense for a persited SparkContext");
    }

    @Override
    public boolean cp(final String fromLocation, final String toLocation) {
        Spark.getRDD(fromLocation).setName(toLocation).cache();
        Spark.removeRDD(fromLocation);
        return true;
    }

    @Override
    public boolean exists(final String location) {
        return Spark.hasRDD(location);
    }

    @Override
    public boolean rm(final String location) {
        if (!Spark.hasRDD(location))
            return false;
        Spark.removeRDD(location);
        return true;
    }

    @Override
    public boolean rmr(final String location) {
        final List<String> rdds = new ArrayList<>();
        final String wildCardLocation = location.replace(".", "\\.").replace("*", ".*");
        for (final RDD<?> rdd : Spark.getRDDs()) {
            if (rdd.name().matches(wildCardLocation))
                rdds.add(rdd.name());
        }
        rdds.forEach(Spark::removeRDD);
        return rdds.size() > 0;
    }

    @Override
    public <V> Iterator<V> head(final String location, final int totalLines, final Class<V> objectClass) {
        return IteratorUtils.limit((Iterator) JavaConversions.asJavaIterator(Spark.getRDD(location).toLocalIterator()), totalLines);
    }

    public String describe(final String location) {
        return Spark.getRDD(location).toDebugString();
    }
}
