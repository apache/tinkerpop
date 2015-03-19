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
import org.apache.spark.api.java.function.Function2;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMapEmitter;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMemory;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkMessagePayload;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkPayload;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkReduceEmitter;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHelper {

    private SparkHelper() {
    }

    public static <M> JavaPairRDD<Object, SparkPayload<M>> executeStep(final JavaPairRDD<Object, SparkPayload<M>> graphRDD, final SparkMemory memory, final Configuration apacheConfiguration) {
        JavaPairRDD<Object, SparkPayload<M>> current = graphRDD;
        // execute vertex program
        current = current.mapPartitionsToPair(partitionIterator -> {     // each partition(Spark)/worker(TP3) has a local copy of the vertex program to reduce object creation
            final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration);
            final Set<String> elementComputeKeys = workerVertexProgram.getElementComputeKeys();
            workerVertexProgram.workerIterationStart(memory);
            final List<Tuple2<Object, SparkPayload<M>>> emission = new ArrayList<>();
            partitionIterator.forEachRemaining(keyValue -> {
                workerVertexProgram.execute(ComputerGraph.of(keyValue._2().asVertexPayload().getVertex(), elementComputeKeys), keyValue._2().asVertexPayload(), memory);
                emission.add(keyValue);
            });
            workerVertexProgram.workerIterationEnd(memory);
            return emission;
        });

        // emit messages by appending them to the graph as message payloads
        current = current.<Object, SparkPayload<M>>flatMapToPair(keyValue -> () -> {
            keyValue._2().asVertexPayload().getMessages().clear(); // the graph vertex should not have any incoming messages (should be cleared from the previous stage)
            return IteratorUtils.concat(
                    IteratorUtils.of(keyValue),                                                                    // this is a vertex
                    IteratorUtils.map(keyValue._2().asVertexPayload().getOutgoingMessages().iterator(),
                            entry -> new Tuple2<>(entry._1(), new SparkMessagePayload<M>(entry._2()))));           // this is a message;
        });

        // "message pass" by merging the message payloads with the vertex payloads
        current = current.reduceByKey(new Function2<SparkPayload<M>, SparkPayload<M>, SparkPayload<M>>() {
            private Optional<MessageCombiner<M>> messageCombinerOptional = null; // a hack to simulate partition(Spark)/worker(TP3) local variables

            @Override
            public SparkPayload<M> call(final SparkPayload<M> payloadA, final SparkPayload<M> payloadB) throws Exception {
                if (null == this.messageCombinerOptional)
                    this.messageCombinerOptional = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration).getMessageCombiner();

                if (payloadA.isVertex()) {
                    payloadA.addMessages(payloadB.getMessages(), this.messageCombinerOptional);
                    return payloadA;
                } else {
                    payloadB.addMessages(payloadA.getMessages(), this.messageCombinerOptional);
                    return payloadB;
                }
            }
        });

        // clear all previous outgoing messages (why can't we do this prior to the shuffle? -- this is probably cause of concurrent modification issues prior to reduceByKey)
        current = current.mapValues(vertexPayload -> {
            vertexPayload.asVertexPayload().getOutgoingMessages().clear();
            return vertexPayload;
        });

        return current;
    }

    public static <K, V, M> JavaPairRDD<K, V> executeMap(final JavaPairRDD<Object, SparkPayload<M>> graphRDD, final MapReduce<K, V, ?, ?, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = graphRDD.mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, ?, ?, ?> workerMapReduce = MapReduce.<MapReduce<K, V, ?, ?, ?>>createMapReduce(apacheConfiguration);
            workerMapReduce.workerStart(MapReduce.Stage.MAP);
            final SparkMapEmitter<K, V> mapEmitter = new SparkMapEmitter<>();
            partitionIterator.forEachRemaining(keyValue -> workerMapReduce.map(keyValue._2().asVertexPayload().getVertex(), mapEmitter));
            workerMapReduce.workerEnd(MapReduce.Stage.MAP);
            return mapEmitter.getEmissions();
        });
        if (mapReduce.getMapKeySort().isPresent())
            mapRDD = mapRDD.sortByKey(mapReduce.getMapKeySort().get());
        return mapRDD;
    }

    // TODO: public static executeCombine()  is this necessary?

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeReduce(final JavaPairRDD<K, V> mapRDD, final MapReduce<K, V, OK, OV, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<OK, OV> reduceRDD = mapRDD.groupByKey().mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, OK, OV, ?> workerMapReduce = MapReduce.<MapReduce<K, V, OK, OV, ?>>createMapReduce(apacheConfiguration);
            workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
            final SparkReduceEmitter<OK, OV> reduceEmitter = new SparkReduceEmitter<>();
            partitionIterator.forEachRemaining(keyValue -> workerMapReduce.reduce(keyValue._1(), keyValue._2().iterator(), reduceEmitter));
            workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
            return reduceEmitter.getEmissions();
        });
        if (mapReduce.getReduceKeySort().isPresent())
            reduceRDD = reduceRDD.sortByKey(mapReduce.getReduceKeySort().get());
        return reduceRDD;
    }

    public static void deleteOutputLocation(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
        if (null != outputLocation) {
            try {
                FileSystem.get(hadoopConfiguration).delete(new Path(outputLocation), true);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public static String getInputLocation(final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        try {
            return FileSystem.get(hadoopConfiguration).getFileStatus(new Path(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION))).getPath().toString();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static <M> void saveGraphRDD(final JavaPairRDD<Object, SparkPayload<M>> graphRDD, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a <nullwritable,vertexwritable> stream for output
            graphRDD.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), new VertexWritable(tuple._2().asVertexPayload().getVertex())))
                    .saveAsNewAPIHadoopFile(outputLocation + "/" + Constants.HIDDEN_G,
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
