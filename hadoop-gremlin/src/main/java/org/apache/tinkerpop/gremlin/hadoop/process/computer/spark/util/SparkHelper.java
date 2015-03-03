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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.util;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMapEmitter;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMemory;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMessenger;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkReduceEmitter;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHelper {

    private SparkHelper() {
    }

    public static <M> JavaPairRDD<Object, SparkMessenger<M>> executeStep(final JavaPairRDD<Object, SparkMessenger<M>> graphRDD, final VertexProgram<M> globalVertexProgram, final SparkMemory memory, final Configuration apacheConfiguration) {
        JavaPairRDD<Object, SparkMessenger<M>> current = graphRDD;
        // execute vertex program
        current = current.mapPartitionsToPair(iterator -> {     // each partition has a copy of the vertex program
            final VertexProgram<M> vertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration);
            return () -> IteratorUtils.<Tuple2<Object, SparkMessenger<M>>, Tuple2<Object, SparkMessenger<M>>>map(iterator, tuple -> {
                vertexProgram.execute(tuple._2().getVertex(), tuple._2(), memory);
                return tuple;
            });
        });
        // clear all previous incoming messages
        if (!memory.isInitialIteration()) {
            current = current.mapValues(messenger -> {
                messenger.clearIncomingMessages();
                return messenger;
            });
        }
        // emit messages
        current = current.<Object, SparkMessenger<M>>flatMapToPair(tuple -> {
            final List<Tuple2<Object, SparkMessenger<M>>> list = tuple._2().getOutgoingMessages()
                    .stream()
                    .map(entry -> new Tuple2<>(entry.getKey(), new SparkMessenger<>(new DetachedVertex(entry.getKey(), Vertex.DEFAULT_LABEL, Collections.emptyMap()), entry.getValue()))) // maybe go back to toy vertex if label is expensive
                    .collect(Collectors.toList());          // the message vertices
            list.add(new Tuple2<>(tuple._1(), tuple._2())); // the raw vertex
            return list;
        });

        // TODO: local message combiner
        if (globalVertexProgram.getMessageCombiner().isPresent()) {
           /* current = current.combineByKey(messenger -> {
                return messenger;
            });*/
        }

        // "message pass" via reduction
        current = current.reduceByKey((a, b) -> {
            if (a.getVertex() instanceof DetachedVertex && !(b.getVertex() instanceof DetachedVertex))
                a.setVertex(b.getVertex());
            a.addIncomingMessages(b);
            return a;
        });

        // clear all previous outgoing messages
        current = current.mapValues(messenger -> {
            messenger.clearOutgoingMessages();
            return messenger;
        });
        return current;
    }

    public static <K, V> JavaPairRDD<K, V> executeMap(final JavaPairRDD<NullWritable, VertexWritable> hadoopGraphRDD, final MapReduce<K, V, ?, ?, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = hadoopGraphRDD.flatMapToPair(tuple -> {
            final MapReduce<K, V, ?, ?, ?> m = MapReduce.createMapReduce(apacheConfiguration);    // todo create only for each partition
            final SparkMapEmitter<K, V> mapEmitter = new SparkMapEmitter<>();
            m.map(tuple._2().get(), mapEmitter);
            return mapEmitter.getEmissions();
        });
        if (mapReduce.getMapKeySort().isPresent())
            mapRDD = mapRDD.sortByKey(mapReduce.getMapKeySort().get());
        return mapRDD;
    }

    // TODO: public static executeCombine()

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeReduce(final JavaPairRDD<K, V> mapRDD, final MapReduce<K, V, OK, OV, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<OK, OV> reduceRDD = mapRDD.groupByKey().flatMapToPair(tuple -> {
            final MapReduce<K, V, OK, OV, ?> m = MapReduce.createMapReduce(apacheConfiguration);     // todo create only for each partition
            final SparkReduceEmitter<OK, OV> reduceEmitter = new SparkReduceEmitter<>();
            m.reduce(tuple._1(), tuple._2().iterator(), reduceEmitter);
            return reduceEmitter.getEmissions();
        });
        if (mapReduce.getReduceKeySort().isPresent())
            reduceRDD = reduceRDD.sortByKey(mapReduce.getReduceKeySort().get());
        return reduceRDD;
    }

    public static void deleteOutputDirectory(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            try {
                FileSystem.get(hadoopConfiguration).delete(new Path(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)), true);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public static <M> void saveVertexProgramRDD(final JavaPairRDD<Object, SparkMessenger<M>> graphRDD, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a <nullwritable,vertexwritable> stream for output
            graphRDD.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), new VertexWritable<>(tuple._2().getVertex())))
                    .saveAsNewAPIHadoopFile(outputLocation + "/" + Constants.SYSTEM_G,
                            NullWritable.class,
                            VertexWritable.class,
                            (Class<OutputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class));
        }
    }

    public static void saveMapReduceRDD(final JavaPairRDD<Object, Object> mapReduceRDD, final MapReduce mapReduce, final Memory.Admin memory, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a Hadoop stream for output
            mapReduceRDD.mapToPair(tuple -> new Tuple2<>(new ObjectWritable<>(tuple._1()), new ObjectWritable<>(tuple._2()))).saveAsNewAPIHadoopFile(outputLocation + "/" + mapReduce.getMemoryKey(),
                    ObjectWritable.class,
                    ObjectWritable.class,
                    (Class<OutputFormat<ObjectWritable, ObjectWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, OutputFormat.class));
            // if its not a SequenceFile there is no certain way to convert to necessary Java objects.
            // to get results you have to look through HDFS directory structure. Oh the horror.
            try {
                if (hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                    mapReduce.addResultToMemory(memory, new ObjectWritableIterator(hadoopConfiguration, new Path(outputLocation + "/" + mapReduce.getMemoryKey())));
                else
                    HadoopGraph.LOGGER.warn(Constants.SEQUENCE_WARNING);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
