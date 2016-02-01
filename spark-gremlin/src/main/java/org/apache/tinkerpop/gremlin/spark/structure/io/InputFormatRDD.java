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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple2;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InputFormatRDD implements InputRDD {

    @Override
    public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(configuration);
        hadoopConfiguration.set(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), Constants.getSearchGraphLocation(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), FileSystemStorage.open(hadoopConfiguration)).get());
        return sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                NullWritable.class,
                VertexWritable.class)
                .mapToPair(tuple -> new Tuple2<>(tuple._2().get().id(), new VertexWritable(tuple._2().get())));
    }

    @Override
    public <K, V> JavaPairRDD<K, V> readMemoryRDD(final Configuration configuration, final String memoryKey, final JavaSparkContext sparkContext) {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(configuration);
        hadoopConfiguration.set(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), Constants.getMemoryLocation(configuration.getString(Constants.GREMLIN_HADOOP_INPUT_LOCATION), memoryKey));
        return sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                SequenceFileInputFormat.class,
                ObjectWritable.class,
                ObjectWritable.class)
                .mapToPair(tuple -> new Tuple2<>((K) ((Tuple2<ObjectWritable, ObjectWritable>) tuple)._1().get(), (V) ((Tuple2<ObjectWritable, ObjectWritable>) tuple)._2().get()));
    }

    public static void storeVertexAndEdgeFilters(final Configuration apacheConfiguration, final org.apache.hadoop.conf.Configuration hadoopConfiguration, final Traversal.Admin<Vertex, Vertex> vertexFilter, final Traversal.Admin<Edge, Edge> edgeFilter) {
        if (null != vertexFilter) {
            VertexProgramHelper.serialize(vertexFilter, apacheConfiguration, Constants.GREMLIN_HADOOP_VERTEX_FILTER);
            hadoopConfiguration.set(Constants.GREMLIN_HADOOP_VERTEX_FILTER, apacheConfiguration.getString(Constants.GREMLIN_HADOOP_VERTEX_FILTER));
        }
        if (null != edgeFilter) {
            VertexProgramHelper.serialize(edgeFilter, apacheConfiguration, Constants.GREMLIN_HADOOP_EDGE_FILTER);
            hadoopConfiguration.set(Constants.GREMLIN_HADOOP_VERTEX_FILTER, apacheConfiguration.getString(Constants.GREMLIN_HADOOP_EDGE_FILTER));
        }
    }
}
