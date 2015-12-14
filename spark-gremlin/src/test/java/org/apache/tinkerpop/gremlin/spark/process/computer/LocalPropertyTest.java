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

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class LocalPropertyTest extends AbstractSparkTest {

    @Test
    public void shouldSetThreadLocalProperties() throws Exception {
        final String testName = "ThreadLocalProperties";
        final String rddName = TestHelper.makeTestDataDirectory(LocalPropertyTest.class) + UUID.randomUUID().toString();
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        configuration.setProperty("spark.jobGroup.id", "22");
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ////////
        SparkConf sparkConfiguration = new SparkConf();
        sparkConfiguration.setAppName(testName);
        ConfUtil.makeHadoopConfiguration(configuration).forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
        JavaSparkContext sparkContext = new JavaSparkContext(SparkContext.getOrCreate(sparkConfiguration));
        JavaSparkStatusTracker statusTracker = sparkContext.statusTracker();
        assertTrue(statusTracker.getJobIdsForGroup("22").length >= 1);
        assertTrue(Spark.hasRDD(rddName));
        ///////
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null);
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        configuration.setProperty("spark.jobGroup.id", "44");
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.NOTHING)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ///////
        assertTrue(statusTracker.getJobIdsForGroup("44").length >= 1);
    }
}

