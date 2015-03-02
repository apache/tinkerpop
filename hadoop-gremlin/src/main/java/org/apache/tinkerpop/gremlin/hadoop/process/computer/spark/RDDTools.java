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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDDTools {

    public static <M> void sendMessage(final Tuple2<Vertex, List<M>> tuple, final M message) {
        tuple._2().add(message);
    }

    public static <M> Iterable<M> receiveMessages(final Tuple2<Vertex, List<M>> tuple) {
        return tuple._2();
    }

    public static <M> JavaPairRDD<Vertex, List<M>> endIteration(final JavaPairRDD<Vertex, List<M>> graph) {
        return graph.flatMapToPair(tuple -> tuple._2().stream().map(message -> new Tuple2<>(tuple._1(), Arrays.asList(message))).collect(Collectors.toList()));
    }

}
