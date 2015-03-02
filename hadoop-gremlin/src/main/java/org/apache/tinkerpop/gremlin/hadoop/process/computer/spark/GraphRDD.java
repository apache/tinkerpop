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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import scala.Tuple2;
import scala.reflect.ManifestFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphRDD<M> extends JavaPairRDD<Vertex, MessageBox<M>> {

    public GraphRDD(final RDD<Tuple2<Vertex, MessageBox<M>>> rdd) {
        super(rdd, ManifestFactory.classType(Vertex.class), ManifestFactory.classType(MessageBox.class));
    }

    public GraphRDD(final JavaPairRDD<Vertex, MessageBox<M>> rdd) {
        super(rdd.rdd(), ManifestFactory.classType(Vertex.class), ManifestFactory.classType(MessageBox.class));
    }

    public GraphRDD completeIteration() {
        JavaPairRDD<Vertex, MessageBox<M>> current = this;
        current = current.mapToPair(tuple -> {
            tuple._2().clearIncomingMessages();
            return tuple;
        });
        current = current.<Vertex, MessageBox<M>>flatMapToPair(tuple -> {
            final List<Tuple2<Vertex, MessageBox<M>>> list = tuple._2().outgoing.entrySet().stream().map(entry -> {
                final Vertex toVertex = new DetachedVertex(entry.getKey(), "vertex", Collections.emptyMap());
                return new Tuple2<>(toVertex, new MessageBox<>(entry.getValue()));
            }).collect(Collectors.toList());
            list.add(new Tuple2<>(tuple._1(), new MessageBox<>()));
            return list;
        });
        current = current.reduceByKey((a, b) -> {
            a.incoming.addAll(b.incoming);
            return a;
        });
        return new GraphRDD<>(current.rdd());
    }

    public static <M> GraphRDD<M> of(final JavaPairRDD<Vertex, MessageBox<M>> javaPairRDD) {
        return new GraphRDD<>(javaPairRDD);
    }

    //////////////

    @Override
    public JavaRDD zipPartitions(JavaRDDLike uJavaRDDLike, FlatMapFunction2 iteratorIteratorVFlatMapFunction2) {
        return (JavaRDD) new JavaRDD<>(null, null);
    }
}
