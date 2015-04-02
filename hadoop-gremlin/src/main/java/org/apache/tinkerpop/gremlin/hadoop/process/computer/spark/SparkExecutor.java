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
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkExecutor {

    private SparkExecutor() {
    }

    ////////////////////
    // VERTEX PROGRAM //
    ////////////////////

    public static <M> JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> executeVertexProgramIteration(
            final JavaPairRDD<Object, VertexWritable> graphRDD,
            final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> viewAndMessagesRDD,
            final SparkMemory memory,
            final Configuration apacheConfiguration) {

        final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<Tuple2<Object, M>>>> viewAndOutgoingMessagesRDD = ((null == viewAndMessagesRDD) ?
                graphRDD.mapValues(vertexWritable -> new Tuple2<>(vertexWritable, Optional.<Tuple2<List<DetachedVertexProperty<Object>>, List<M>>>absent())) : // first iteration will not have any views or messages
                graphRDD.leftOuterJoin(viewAndMessagesRDD))                                                                                                    // every other iteration may have views and messages
                // for each partition of vertices
                .mapPartitionsToPair(partitionIterator -> {
                    final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration); // each partition(Spark)/worker(TP3) has a local copy of the vertex program to reduce object creation
                    final Set<String> elementComputeKeys = workerVertexProgram.getElementComputeKeys(); // the compute keys as a set
                    final String[] elementComputeKeysArray = elementComputeKeys.size() == 0 ? null : elementComputeKeys.toArray(new String[elementComputeKeys.size()]); // the compute keys as an array
                    workerVertexProgram.workerIterationStart(memory); // start the worker
                    return () -> IteratorUtils.map(partitionIterator, vertexViewAndMessages -> {
                        final Vertex vertex = vertexViewAndMessages._2()._1().get();
                        final boolean hasViewAndMessages = vertexViewAndMessages._2()._2().isPresent(); // if this is the first iteration, then there are no views or messages
                        final List<DetachedVertexProperty<Object>> previousView = hasViewAndMessages ? vertexViewAndMessages._2()._2().get()._1() : Collections.emptyList();
                        final List<M> incomingMessages = hasViewAndMessages ? vertexViewAndMessages._2()._2().get()._2() : Collections.emptyList();
                        previousView.forEach(property -> DetachedVertexProperty.addTo(vertex, property));  // attach the view to the vertex
                        ///
                        final SparkMessenger<M> messenger = new SparkMessenger<>(vertex, incomingMessages); // create the messenger with the incoming messages
                        workerVertexProgram.execute(ComputerGraph.of(vertex, elementComputeKeys), messenger, memory); // execute the vertex program on this vertex
                        ///
                        final List<DetachedVertexProperty<Object>> nextView = null == elementComputeKeysArray ?  // not all vertex programs have compute keys
                                Collections.emptyList() :
                                IteratorUtils.list(IteratorUtils.map(vertex.properties(elementComputeKeysArray), property -> DetachedFactory.detach(property, true)));
                        final List<Tuple2<Object, M>> outgoingMessages = messenger.getOutgoingMessages(); // get the outgoing messages
                        if (!partitionIterator.hasNext())
                            workerVertexProgram.workerIterationEnd(memory); // if no more vertices in the partition, end the worker's iteration
                        return new Tuple2<>(vertex.id(), new Tuple2<>(nextView, outgoingMessages));
                    });
                });

        viewAndOutgoingMessagesRDD.cache();  // will use twice (once for message passing and once for view isolation)

        // "message pass" by reducing on the vertex object id of the message payloads
        final MessageCombiner<M> messageCombiner = VertexProgram.<VertexProgram<M>>createVertexProgram(apacheConfiguration).getMessageCombiner().orElse(null);
        final JavaPairRDD<Object, List<M>> incomingMessagesRDD = viewAndOutgoingMessagesRDD
                .mapValues(Tuple2::_2)
                .flatMapToPair(tuple -> () -> IteratorUtils.map(tuple._2().iterator(), message -> {
                    final List<M> list = (null == messageCombiner) ? new ArrayList<>() : new ArrayList<>(1);
                    list.add(message._2());
                    return new Tuple2<>(message._1(), list);
                })).reduceByKey((a, b) -> {
                    if (null == messageCombiner) {
                        a.addAll(b);
                        return a;
                    } else {
                        a.set(0, messageCombiner.combine(a.get(0), b.get(0)));
                        return a;
                    }
                });

        // isolate the views and then join the incoming messages
        final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> viewAndIncomingMessagesRDD = viewAndOutgoingMessagesRDD
                .mapValues(Tuple2::_1)
                .leftOuterJoin(incomingMessagesRDD) // there will always be views (even if empty), but there will not always be incoming messages
                .mapValues(tuple -> new Tuple2<>(tuple._1(), tuple._2().or(Collections.emptyList())));

        viewAndIncomingMessagesRDD.foreachPartition(partitionIterator -> {
        }); // need to complete a task so its BSP and the memory for this iteration is updated
        return viewAndIncomingMessagesRDD;
    }

    /////////////////
    // MAP REDUCE //
    ////////////////

    public static <M> JavaPairRDD<Object, VertexWritable> prepareGraphRDDForMapReduce(final JavaPairRDD<Object, VertexWritable> graphRDD, final JavaPairRDD<Object, Tuple2<List<DetachedVertexProperty<Object>>, List<M>>> viewAndMessagesRDD) {
        return (null == viewAndMessagesRDD) ?
                graphRDD.mapValues(vertexWritable -> {
                    vertexWritable.get().edges(Direction.BOTH).forEachRemaining(Edge::remove);
                    return vertexWritable;
                }) :
                graphRDD.leftOuterJoin(viewAndMessagesRDD)
                        .mapValues(tuple -> {
                            final Vertex vertex = tuple._1().get();
                            vertex.edges(Direction.BOTH).forEachRemaining(Edge::remove);
                            final List<DetachedVertexProperty<Object>> view = tuple._2().isPresent() ? tuple._2().get()._1() : Collections.emptyList();
                            view.forEach(property -> DetachedVertexProperty.addTo(vertex, property));
                            return tuple._1();
                        });
    }

    public static <K, V> JavaPairRDD<K, V> executeMap(final JavaPairRDD<Object, VertexWritable> graphRDD, final MapReduce<K, V, ?, ?, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = graphRDD.mapPartitionsToPair(partitionIterator -> {
            final MapReduce<K, V, ?, ?, ?> workerMapReduce = MapReduce.<MapReduce<K, V, ?, ?, ?>>createMapReduce(apacheConfiguration);
            workerMapReduce.workerStart(MapReduce.Stage.MAP);
            final SparkMapEmitter<K, V> mapEmitter = new SparkMapEmitter<>();
            return () -> IteratorUtils.flatMap(partitionIterator, vertexWritable -> {
                workerMapReduce.map(vertexWritable._2().get(), mapEmitter);
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

    ///////////////////
    // Input/Output //
    //////////////////

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

    public static <M> void saveGraphRDD(final JavaPairRDD<Object, VertexWritable> graphRDD, final org.apache.hadoop.conf.Configuration hadoopConfiguration) {
        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
        if (null != outputLocation) {
            // map back to a <nullwritable,vertexwritable> stream for output
            graphRDD.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), tuple._2()))
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
                    mapReduce.addResultToMemory(memory, mapReduceRDD.map(tuple -> new KeyValue<>(tuple._1(), tuple._2())).collect().iterator());
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
