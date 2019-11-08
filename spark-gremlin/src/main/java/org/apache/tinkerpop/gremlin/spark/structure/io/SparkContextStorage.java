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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

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
        return ls(ROOT_DIRECTORY);
    }

    @Override
    public List<String> ls(final String pattern) {
        final List<String> rdds = new ArrayList<>();

        forEachMatching( pattern, rdd -> rdds.add(rdd.name() + " [" + rdd.getStorageLevel().description() + "]"));

        return rdds;
    }


    /**
     * @param pattern not null {@link Storage} file system pattern
     * @param action not null processing of each RDD matching the pattern
     */
    private static void forEachMatching(final String pattern, Consumer<RDD<?>> action) {
        final Pattern storagePattern = toRegularExpression(toPattern(Storage.toPath(pattern)));

        for (final RDD<?> rdd : Spark.getRDDs()) {
            if (storagePattern.matcher(Storage.toPath(rdd.name())).matches() ) {
                action.accept( rdd );
            }
        }
    }

    /**
     * @param pattern not null {@link Storage} file system pattern
     * @return true if there is at least one RDD matching the pattern
     */
    private static boolean thereIsMatching(final String pattern) {
        final Pattern storagePattern = toRegularExpression(toPattern(Storage.toPath(pattern)));

        for (final RDD<?> rdd : Spark.getRDDs()) {
            if (storagePattern.matcher(Storage.toPath(rdd.name())).matches() ) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param location not null path or pattern in the abstract {@link Storage}
     * @return a pattern abstract {@link Storage} that matches all paths starting with location
     */
    private static String toPattern(final String location) {
        return location.endsWith("*") ? location : location + "*";
    }

    /**
     * @param storagePattern not null, not empty pattern in the abstract {@link Storage}
     * @return non-null regular expression to match local file system paths as strings
     */
    private static Pattern toRegularExpression(final String storagePattern) {
        return Pattern.compile(storagePattern.replace(".", "\\.")
                                             .replace("?", ".")
                                             .replace("*", ".*"));
    }

    /**
     * @param pattern
     * @return true if the pattern is actually a path as of the path definition in {@linkplain Storage}
     *
     * NOTE: a more precise syntax check is possible
     */
    private static boolean isPath(String pattern) {
      return pattern != null
             && !pattern.isEmpty()
             && !pattern.contains( "*" )
             && !pattern.contains( "?" );
    }

    @Override
    public boolean cp(final String sourceLocation, final String targetLocation) {
        final Pattern replacementPattern;
        final List<String> rdds = new ArrayList<>();

        forEachMatching( sourceLocation, rdd -> rdds.add(rdd.name()));

        if (rdds.size() == 0)
            return false;

        replacementPattern = toRegularExpression( sourceLocation );
        for (final String rddName : rdds) {
            final String newLocation;

            newLocation = replacementPattern.matcher( rddName ).replaceFirst( targetLocation );

            Spark.getRDD(rddName).toJavaRDD().filter(a -> true)  // a clone of the source RDD
                 .setName(newLocation)
                 .cache()
                 .count();
        }
        return true;
    }

    @Override
    public boolean exists(final String location) {
        return thereIsMatching(location);
    }

    @Override
    public boolean rm(final String pattern) {
        final List<String> rdds = new ArrayList<>();

        forEachMatching( pattern, rdd -> rdds.add(rdd.name()));

        rdds.forEach(Spark::removeRDD);
        return rdds.size() > 0;
    }

    @Override
    public Iterator<Vertex> head(final String location, final Class readerClass, final int totalLines) {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, location);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, readerClass.getCanonicalName());
        try {
            if (InputRDD.class.isAssignableFrom(readerClass)) {
                return IteratorUtils.map(((InputRDD) readerClass.getConstructor().newInstance()).readGraphRDD(configuration, new JavaSparkContext(Spark.getContext())).take(totalLines).iterator(), tuple -> tuple._2().get());
            } else if (InputFormat.class.isAssignableFrom(readerClass)) {
                return IteratorUtils.map(new InputFormatRDD().readGraphRDD(configuration, new JavaSparkContext(Spark.getContext())).take(totalLines).iterator(), tuple -> tuple._2().get());
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        throw new IllegalArgumentException("The provided parserClass must be an " + InputFormat.class.getCanonicalName() + " or an " + InputRDD.class.getCanonicalName() + ": " + readerClass.getCanonicalName());
    }

    @Override
    public <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class readerClass, final int totalLines) {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, location);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, readerClass.getCanonicalName());
        try {
            if (InputRDD.class.isAssignableFrom(readerClass)) {
                return IteratorUtils.map(((InputRDD) readerClass.getConstructor().newInstance()).readMemoryRDD(configuration, memoryKey, new JavaSparkContext(Spark.getContext())).take(totalLines).iterator(), tuple -> new KeyValue(tuple._1(), tuple._2()));
            } else if (InputFormat.class.isAssignableFrom(readerClass)) {
                return IteratorUtils.map(new InputFormatRDD().readMemoryRDD(configuration, memoryKey, new JavaSparkContext(Spark.getContext())).take(totalLines).iterator(), tuple -> new KeyValue(tuple._1(), tuple._2()));
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        throw new IllegalArgumentException("The provided parserClass must be an " + InputFormat.class.getCanonicalName() + " or an " + InputRDD.class.getCanonicalName() + ": " + readerClass.getCanonicalName());
    }

    @Override
    public Iterator<String> head(final String location, final int totalLines) {
        return IteratorUtils.map(Spark.getRDD(location).toJavaRDD().take(totalLines).iterator(), Object::toString);
    }

    public String describe(final String location) {
        return Spark.getRDD(location).toDebugString();
    }

    @Override
    public String toString() {
        return StringFactory.storageString(null == Spark.getContext() ? "spark:none" : Spark.getContext().master());
    }
}
