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

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ToyGraphInputRDD implements InputRDD {

    public static final String GREMLIN_SPARK_TOY_GRAPH = "gremlin.spark.toyGraph";

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        final List<Vertex> vertices;
        if (configuration.getProperty(GREMLIN_SPARK_TOY_GRAPH).equals(LoadGraphWith.GraphData.MODERN.toString()))
            vertices = IteratorUtils.list(TinkerFactory.createModern().vertices());
        else if (configuration.getProperty(GREMLIN_SPARK_TOY_GRAPH).equals(LoadGraphWith.GraphData.CLASSIC.toString()))
            vertices = IteratorUtils.list(TinkerFactory.createClassic().vertices());
        else if (configuration.getProperty(GREMLIN_SPARK_TOY_GRAPH).equals(LoadGraphWith.GraphData.CREW.toString()))
            vertices = IteratorUtils.list(TinkerFactory.createTheCrew().vertices());
        else if (configuration.getProperty(GREMLIN_SPARK_TOY_GRAPH).equals(LoadGraphWith.GraphData.GRATEFUL.toString())) {
            try {
                final Graph graph = TinkerGraph.open();
                graph.io(GryoIo.build()).readGraph(GryoResourceAccess.class.getResource("grateful-dead.kryo").getFile());
                vertices = IteratorUtils.list(graph.vertices());
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } else
            throw new IllegalArgumentException("No legal toy graph was provided to load: " + configuration.getProperty(GREMLIN_SPARK_TOY_GRAPH));

        return sparkContext.parallelize(vertices.stream().map(VertexWritable::new).collect(Collectors.toList())).mapToPair(vertex -> new Tuple2<>(vertex.get().id(), vertex));
    }
}
