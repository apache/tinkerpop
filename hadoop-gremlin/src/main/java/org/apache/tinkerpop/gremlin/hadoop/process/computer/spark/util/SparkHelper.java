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
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHelper {

    private SparkHelper() {
    }

    public static <M> JavaPairRDD<Object, SparkMessenger<M>> executeStep(final JavaPairRDD<Object, SparkMessenger<M>> graphRDD, final VertexProgram<M> globalVertexProgram, final SparkMemory memory, final Configuration apacheConfiguration) {
        JavaPairRDD<Object, SparkMessenger<M>> current = graphRDD;
        // execute vertex program
        current = current.mapPartitionsToPair(partitionIterator -> {     // each partition(Spark)/worker(TP3) has a local copy of the vertex program to reduce object creation
            final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration);
            workerVertexProgram.workerIterationStart(memory);
            return () -> IteratorUtils.<Tuple2<Object, SparkMessenger<M>>, Tuple2<Object, SparkMessenger<M>>>map(partitionIterator, keyValue -> {
                workerVertexProgram.execute(keyValue._2().getVertex(), keyValue._2(), memory);
                if (!partitionIterator.hasNext()) workerVertexProgram.workerIterationEnd(memory);  // is this safe?
                return keyValue;
            });
        });

        // emit messages by appending them to the graph vertices as message "vertices"
        current = current.<Object, SparkMessenger<M>>flatMapToPair(keyValue -> () -> {
            keyValue._2().clearIncomingMessages(); // the graph vertex should not have any incoming messages (should be cleared from the previous stage)
            return IteratorUtils.<Tuple2<Object, SparkMessenger<M>>>concat(
                    IteratorUtils.of(keyValue),
                    IteratorUtils.map(keyValue._2().getOutgoingMessages().iterator(),                                      // this is the graph vertex
                            entry -> new Tuple2<>(entry._1(), SparkMessenger.forMessageVertex(entry._2(), entry._2()))));  // this is a message "vertex";
        });

        // "message pass" via reduction joining the "message vertices" with the graph vertices
        // addIncomingMessages is provided the vertex program message combiner for partition and global level combining
        final boolean hasMessageCombiner = globalVertexProgram.getMessageCombiner().isPresent();
        current = current.reduceByKey((messengerA, messengerB) -> {
            final Optional<MessageCombiner<M>> messageCombinerOptional = hasMessageCombiner ?
                    VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration).getMessageCombiner() :  // this is expensive but there is no "reduceByKeyPartition" :(
                    Optional.empty();

            if (messengerA.getVertex() instanceof DetachedVertex && !(messengerB.getVertex() instanceof DetachedVertex)) // fold the message vertices into the graph vertices
                messengerA.mergeInMessenger(messengerB);
            messengerA.addIncomingMessages(messengerB, messageCombinerOptional);

            return messengerA;  // always reduce on the first argument
        });

        // clear all previous outgoing messages (why can't we do this prior to the shuffle?)
        current = current.mapValues(messenger -> {
            messenger.clearOutgoingMessages();
            return messenger;
        });

        return current;
    }

    public static <K, V> JavaPairRDD<K, V> executeMap(final JavaPairRDD<NullWritable, VertexWritable> hadoopGraphRDD, final MapReduce<K, V, ?, ?, ?> globalMapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = hadoopGraphRDD.mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, ?, ?, ?> workerMapReduce = MapReduce.createMapReduce(apacheConfiguration);
            final SparkMapEmitter<K, V> mapEmitter = new SparkMapEmitter<>();
            partitionIterator.forEachRemaining(keyValue -> workerMapReduce.map(keyValue._2().get(), mapEmitter));
            return mapEmitter.getEmissions();
        });
        if (globalMapReduce.getMapKeySort().isPresent())
            mapRDD = mapRDD.sortByKey(globalMapReduce.getMapKeySort().get());
        return mapRDD;
    }

    // TODO: public static executeCombine()

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeReduce(final JavaPairRDD<K, V> mapRDD, final MapReduce<K, V, OK, OV, ?> globalMapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<OK, OV> reduceRDD = mapRDD.groupByKey().mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, OK, OV, ?> workerMapReduce = MapReduce.createMapReduce(apacheConfiguration);
            final SparkReduceEmitter<OK, OV> reduceEmitter = new SparkReduceEmitter<>();
            partitionIterator.forEachRemaining(keyValue -> workerMapReduce.reduce(keyValue._1(), keyValue._2().iterator(), reduceEmitter));
            return reduceEmitter.getEmissions();
        });
        if (globalMapReduce.getReduceKeySort().isPresent())
            reduceRDD = reduceRDD.sortByKey(globalMapReduce.getReduceKeySort().get());
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
            mapReduceRDD.mapToPair(keyValue -> new Tuple2<>(new ObjectWritable<>(keyValue._1()), new ObjectWritable<>(keyValue._2()))).saveAsNewAPIHadoopFile(outputLocation + "/" + mapReduce.getMemoryKey(),
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
