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

import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExampleInputRDD implements InputRDD {

    static {
        InputOutputHelper.registerInputOutputPair(ExampleInputRDD.class, ExampleOutputRDD.class);
    }

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        final List<Vertex> list = new ArrayList<>();
        list.add(StarGraph.open().addVertex(T.id, 1l, T.label, "person", "age", 29));
        list.add(StarGraph.open().addVertex(T.id, 2l, T.label, "person", "age", 27));
        list.add(StarGraph.open().addVertex(T.id, 4l, T.label, "person", "age", 32));
        list.add(StarGraph.open().addVertex(T.id, 6l, T.label, "person", "age", 35));
        return sparkContext.parallelize(list).mapToPair(vertex -> new Tuple2<>(vertex.id(), new VertexWritable(vertex)));
    }
}