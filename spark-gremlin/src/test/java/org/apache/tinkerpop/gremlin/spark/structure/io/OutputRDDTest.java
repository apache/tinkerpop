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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OutputRDDTest extends AbstractSparkTest {

    @Test
    public void shouldWriteToArbitraryRDD() throws Exception {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("tinkerpop-modern-v3.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, ExampleOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(this.getClass(), "shouldWriteToArbitraryRDD"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        ////////
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(Computer.compute(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
    }
}
