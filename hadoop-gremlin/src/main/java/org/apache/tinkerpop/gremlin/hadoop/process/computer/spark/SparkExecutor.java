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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import com.google.common.base.Optional;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkExecutor {

    private SparkExecutor() {
    }

    public static <M> JavaPairRDD<Object, SparkVertexPayload<M>> executeVertexProgramIteration(final JavaPairRDD<Object, SparkVertexPayload<M>> graphRDD, final SparkMemory memory, final Configuration apacheConfiguration) {
        // execute vertex program iteration
        final JavaPairRDD<Object, SparkVertexPayload<M>> verticesHoldingOutgoingMessages = graphRDD.mapPartitionsToPair(partitionIterator -> {     // each partition(Spark)/worker(TP3) has a local copy of the vertex program to reduce object creation
            final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration);
            final Set<String> elementComputeKeys = workerVertexProgram.getElementComputeKeys();
            workerVertexProgram.workerIterationStart(memory);
            return () -> IteratorUtils.map(partitionIterator, vertex -> {
                vertex._2().getOutgoingMessages().clear(); // there should be no outgoing messages at this point
                workerVertexProgram.execute(ComputerGraph.of(vertex._2().getVertex(), elementComputeKeys), vertex._2(), memory);
                if (!partitionIterator.hasNext())
                    workerVertexProgram.workerIterationEnd(memory);
                vertex._2().getMessages().clear(); // there should be no incoming messages at this point (only outgoing messages)
                return vertex;
            });
        });

        // emit messages by appending them to the graph as message payloads
        final JavaPairRDD<Object, SparkPayload<M>> verticesAndOutgoingMessages = verticesHoldingOutgoingMessages.flatMapToPair(vertex -> () ->
                IteratorUtils.<Tuple2<Object, SparkPayload<M>>>concat(
                        (Iterator) IteratorUtils.of(vertex),
                        (Iterator) IteratorUtils.map(vertex._2().detachOutgoingMessages(), // this removes all outgoing messages once they have been iterated by this step
                                message -> new Tuple2<>(message._1(), new SparkMessagePayload<>(message._2())))));

        // "message pass" by merging the message payloads with the vertex payloads
        final MessageCombiner<M> messageCombiner = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration).getMessageCombiner().orElse(null);
        final JavaPairRDD<Object, SparkVertexPayload<M>> verticesHoldingIncomingMessages = (JavaPairRDD) verticesAndOutgoingMessages
                .reduceByKey((payloadA, payloadB) -> {
                    if (payloadA.isVertex()) {
                        if (payloadB.isVertex())
                            throw new IllegalStateException("It should not be the case that two vertices reduce to the same key: " + payloadA.asVertexPayload().getVertex() + "==" + payloadB.asVertexPayload().getVertex());
                        else {
                            payloadA.addMessages(payloadB.getMessages(), messageCombiner);
                            return payloadA;
                        }
                    } else {
                        payloadB.addMessages(payloadA.getMessages(), messageCombiner);
                        return payloadB;
                    }
                })
                .filter(payload -> payload._2().isVertex());  // just in case there are messages sent to vertices that do not exist

        verticesHoldingIncomingMessages.foreachPartition(partitionIterator -> {
        }); // need to complete a task so its BSP.
        return verticesHoldingIncomingMessages;
    }

    //////////////////////////////
    /////DEMO ALGORITHM /////////
    /////////////////////////////
    /////////////////////////////

    public static <M> JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> executeVertexProgramIteration2(final JavaPairRDD<Object, VertexWritable> graphRDD, final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> viewMessageRDD, final SparkMemory memory, final Configuration apacheConfiguration) {
        final JavaPairRDD<Object, Tuple2<VertexWritable, Optional<Tuple2<List<DetachedVertexProperty<Object>>, List<M>>>>> graphViewMessagesRDD = graphRDD.leftOuterJoin(viewMessageRDD);

        final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<Tuple2<Object, M>>>> viewAndMessagesRDD = graphViewMessagesRDD.mapPartitionsToPair(partitions -> {
            final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration);
            workerVertexProgram.workerIterationStart(memory);
            final SparkMessenger<M> messenger = new SparkMessenger<>();
            final Set<String> elementComputeKeys = workerVertexProgram.getElementComputeKeys();
            final String[] elementComputeKeysArray = elementComputeKeys.toArray(new String[elementComputeKeys.size()]);
            return () -> IteratorUtils.map(partitions, graphViewMessages -> {
                final Vertex vertex = graphViewMessages._2()._1().get();
                final List<DetachedVertexProperty<Object>> view = graphViewMessages._2()._2().isPresent() ? graphViewMessages._2()._2().get()._1() : Collections.emptyList();
                final List<M> incomingMessages = graphViewMessages._2()._2().isPresent() ? graphViewMessages._2()._2().get()._2() : Collections.emptyList();
                view.forEach(property -> property.attach(vertex));
                messenger.setVertexAndMessages(vertex, incomingMessages);
                memory.setInTask(true);
                workerVertexProgram.execute(vertex, messenger, memory);
                memory.setInTask(false);
                final List<DetachedVertexProperty<Object>> properties = IteratorUtils.list(IteratorUtils.<VertexProperty<Object>, DetachedVertexProperty<Object>>map(vertex.properties(elementComputeKeysArray), property -> DetachedFactory.detach(property, true)));
                vertex.properties(elementComputeKeysArray).forEachRemaining(VertexProperty::remove);
                if (!partitions.hasNext())
                    workerVertexProgram.workerIterationEnd(memory);
                return new Tuple2<>(vertex.id(), new Tuple2<>(properties, messenger.getOutgoingMessages()));
            });
        });

        final JavaPairRDD<Object, List<DetachedVertexProperty<Object>>> newViewRDD = viewAndMessagesRDD.mapValues(Tuple2::_1);

        final MessageCombiner<M> messageCombiner = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration).getMessageCombiner().orElse(null);
        final JavaPairRDD<Object, List<M>> newMessagesRDD = viewAndMessagesRDD
                .flatMapToPair(viewAndMessages -> () -> viewAndMessages._2()._2().iterator())
                .mapValues(message -> {
                    final List<M> list = new ArrayList<>(1);
                    list.add(message);
                    return list;
                }).reduceByKey((messageA, messageB) -> {
                    if (null != messageCombiner) {
                        final M message = Stream.concat(messageA.stream(), messageB.stream()).reduce(messageCombiner::combine).get();
                        messageA.clear();
                        messageA.add(message);
                        return messageA;
                    } else {
                        messageA.addAll(messageB);
                        return messageA;
                    }
                });

        final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> newViewMessages = newViewRDD.join(newMessagesRDD);

        newViewMessages.foreachPartition(x -> {
        }); // execute the view

        return newViewMessages;
    }

    public static <K, V, M> JavaPairRDD<K, V> executeMap(final JavaPairRDD<Object, SparkVertexPayload<M>> graphRDD, final MapReduce<K, V, ?, ?, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = graphRDD.mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, ?, ?, ?> workerMapReduce = MapReduce.<MapReduce<K, V, ?, ?, ?>>createMapReduce(apacheConfiguration);
            workerMapReduce.workerStart(MapReduce.Stage.MAP);
            final SparkMapEmitter<K, V> mapEmitter = new SparkMapEmitter<>();
            return () -> IteratorUtils.flatMap(partitionIterator, keyValue -> {
                workerMapReduce.map(keyValue._2().getVertex(), mapEmitter);
                if (!partitionIterator.hasNext())
                    workerMapReduce.workerEnd(MapReduce.Stage.MAP);
                return mapEmitter.getEmissions();
            });
        });
        if (mapReduce.getMapKeySort().isPresent())
            mapRDD = mapRDD.sortByKey(mapReduce.getMapKeySort().get());
        return mapRDD;
    }

    // TODO: public static executeCombine()  is this necessary?  YES --- we groupByKey in reduce() where we want to combine first.

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeReduce(final JavaPairRDD<K, V> mapRDD, final MapReduce<K, V, OK, OV, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<OK, OV> reduceRDD = mapRDD.groupByKey().mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, OK, OV, ?> workerMapReduce = MapReduce.<MapReduce<K, V, OK, OV, ?>>createMapReduce(apacheConfiguration);
            workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
            final SparkReduceEmitter<OK, OV> reduceEmitter = new SparkReduceEmitter<>();
            return () -> IteratorUtils.flatMap(partitionIterator, keyValue -> {
                workerMapReduce.reduce(keyValue._1(), keyValue._2().iterator(), reduceEmitter);
                if (!partitionIterator.hasNext())
                    workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
                return reduceEmitter.getEmissions();
            });
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

    public static <M> void saveGraphRDD(final JavaPairRDD<Object, SparkVertexPayload<M>> graphRDD, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a <nullwritable,vertexwritable> stream for output
            graphRDD.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), tuple._2().getVertexWritable()))
                    .saveAsNewAPIHadoopFile(outputLocation + "/" + Constants.HIDDEN_G,
                            NullWritable.class,
                            VertexWritable.class,
                            (Class<OutputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class), hadoopConfiguration);
        }
    }

    public static void saveMapReduceRDD(final JavaPairRDD<Object, Object> mapReduceRDD, final MapReduce mapReduce, final Memory.Admin memory, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a Hadoop stream for output
            mapReduceRDD.mapToPair(keyValue -> new Tuple2<>(new ObjectWritable<>(keyValue._1()), new ObjectWritable<>(keyValue._2()))).saveAsNewAPIHadoopFile(outputLocation + "/" + mapReduce.getMemoryKey(),
                    ObjectWritable.class,
                    ObjectWritable.class,
                    (Class<OutputFormat<ObjectWritable, ObjectWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, OutputFormat.class), hadoopConfiguration);
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
