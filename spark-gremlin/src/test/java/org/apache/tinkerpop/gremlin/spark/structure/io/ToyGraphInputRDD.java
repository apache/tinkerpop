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
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ToyGraphInputRDD implements InputRDD {

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        HadoopPools.initialize(TinkerGraph.open().configuration());
        final List<VertexWritable> vertices;
        if (configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION).contains("modern"))
            vertices = IteratorUtils.list(IteratorUtils.map(TinkerFactory.createModern().vertices(), VertexWritable::new));
        else if (configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION).contains("classic"))
            vertices = IteratorUtils.list(IteratorUtils.map(TinkerFactory.createClassic().vertices(), VertexWritable::new));
        else if (configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION).contains("crew"))
            vertices = IteratorUtils.list(IteratorUtils.map(TinkerFactory.createTheCrew().vertices(), VertexWritable::new));
        else if (configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION).contains("grateful")) {
            try {
                final Graph graph = TinkerGraph.open();
                final GraphReader reader = GryoReader.build().mapper(graph.io(GryoIo.build()).mapper().create()).create();
                try (final InputStream stream = GryoResourceAccess.class.getResourceAsStream("grateful-dead.kryo")) {
                    reader.readGraph(stream, graph);
                }
                vertices = IteratorUtils.list(IteratorUtils.map(graph.vertices(), VertexWritable::new));
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } else
            throw new IllegalArgumentException("No legal toy graph was provided to load: " + configuration.getProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION));

        return sparkContext.parallelize(vertices).mapToPair(vertex -> new Tuple2<>(vertex.get().id(), vertex));
    }
}
