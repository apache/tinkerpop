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

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import scala.Tuple2;
import scala.reflect.ManifestFactory;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphComputerRDD<M> extends JavaPairRDD<Object, SparkMessenger<M>> {

    public GraphComputerRDD(final RDD<Tuple2<Object, SparkMessenger<M>>> rdd) {
        super(rdd, ManifestFactory.classType(Object.class), ManifestFactory.classType(SparkMessenger.class));
    }

    public GraphComputerRDD(final JavaPairRDD<Object, SparkMessenger<M>> rdd) {
        super(rdd.rdd(), ManifestFactory.classType(Object.class), ManifestFactory.classType(SparkMessenger.class));
    }

    public GraphComputerRDD completeIteration() {
        JavaPairRDD<Object, SparkMessenger<M>> current = this;
        // clear all previous incoming messages
        current = current.mapValues(messenger -> {
            messenger.clearIncomingMessages();
            return messenger;
        });
        // emit messages
        current = current.<Object, SparkMessenger<M>>flatMapToPair(tuple -> {
            final List<Tuple2<Object, SparkMessenger<M>>> list = tuple._2().outgoing.entrySet()
                    .stream()
                    .map(entry -> new Tuple2<>(entry.getKey(), new SparkMessenger<>(new ToyVertex(entry.getKey()), entry.getValue())))
                    .collect(Collectors.toList());
            list.add(new Tuple2<>(tuple._1(), tuple._2()));
            return list;
        });
        // "message pass" via reduction
        current = current.reduceByKey((a, b) -> {
            if (a.vertex instanceof ToyVertex && !(b.vertex instanceof ToyVertex))
                a.vertex = b.vertex;
            a.incoming.addAll(b.incoming);
            return a;
        });
        // clear all previous outgoing messages
        current = current.mapValues(messenger -> {
            messenger.clearOutgoingMessages();
            return messenger;
        });
        current.count(); // TODO: necessary for BSP?
        return GraphComputerRDD.of(current);
    }

    private static void doNothing() {

    }

    public GraphComputerRDD execute(final Configuration configuration, final SparkMemory memory) {
        JavaPairRDD<Object, SparkMessenger<M>> current = this;
        current = current.mapValues(messenger -> {
            VertexProgram.createVertexProgram(configuration).execute(messenger.vertex, messenger, memory);
            return messenger;
        });
        return GraphComputerRDD.of(current);
    }

    public static <M> GraphComputerRDD<M> of(final JavaPairRDD<Object, SparkMessenger<M>> javaPairRDD) {
        return new GraphComputerRDD<>(javaPairRDD);
    }

    public static <M> GraphComputerRDD<M> of(final JavaRDD<Tuple2<Object, SparkMessenger<M>>> javaRDD) {
        return new GraphComputerRDD<>(javaRDD.rdd());
    }

    //////////////

    @Override
    public JavaRDD zipPartitions(JavaRDDLike uJavaRDDLike, FlatMapFunction2 iteratorIteratorVFlatMapFunction2) {
        return (JavaRDD) new JavaRDD<>(null, null);
    }

}
