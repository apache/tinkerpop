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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InputRDDTest  extends AbstractSparkTest {

    @Test
    public void shouldReadFromArbitraryRDD() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, ExampleInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(this.getClass(), "shouldReadFromArbitraryRDD"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        ////////
        Graph graph = GraphFactory.open(configuration);
        assertEquals(123l, graph.traversal(GraphTraversalSource.computer(SparkGraphComputer.class)).V().values("age").sum().next());
        assertEquals(Long.valueOf(4l), graph.traversal(GraphTraversalSource.computer(SparkGraphComputer.class)).V().count().next());
    }

    @Test
    public void shouldSupportHadoopGraphOLTP() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, ExampleInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputRDDFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(this.getClass(), "shouldSupportHadoopGraphOLTP"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        ////////
        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal(); // OLTP;
        assertEquals("person", g.V().has("age", 29).next().label());
        assertEquals(Long.valueOf(4), g.V().count().next());
        assertEquals(Long.valueOf(0), g.E().count().next());
        assertEquals(Long.valueOf(2), g.V().has("age", P.gt(30)).count().next());
    }
}
