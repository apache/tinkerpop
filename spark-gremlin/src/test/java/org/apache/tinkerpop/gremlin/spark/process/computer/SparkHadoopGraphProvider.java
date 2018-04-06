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

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.AbstractFileGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorageCheck;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.SparkContextStorageCheck;
import org.apache.tinkerpop.gremlin.spark.structure.io.ToyGraphInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@GraphProvider.Descriptor(computer = SparkGraphComputer.class)
public class SparkHadoopGraphProvider extends AbstractFileGraphProvider {

    static final String PREVIOUS_SPARK_PROVIDER = "previous.spark.provider";

    public static final Set<Class> IMPLEMENTATION = Collections.unmodifiableSet(new HashSet<Class>() {{
        add(HadoopEdge.class);
        add(HadoopElement.class);
        add(HadoopGraph.class);
        add(HadoopProperty.class);
        add(HadoopVertex.class);
        add(HadoopVertexProperty.class);
        add(ComputerGraph.class);
        add(ComputerGraph.ComputerElement.class);
        add(ComputerGraph.ComputerVertex.class);
        add(ComputerGraph.ComputerEdge.class);
        add(ComputerGraph.ComputerVertexProperty.class);
        add(ComputerGraph.ComputerAdjacentVertex.class);
        add(ComputerGraph.ComputerProperty.class);
    }});

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) ((HadoopGraph) graph).configuration().setInputLocation(getInputLocation(graph, loadGraphWith.value()));
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        this.graphSONInput = RANDOM.nextBoolean();
        if (this.getClass().equals(SparkHadoopGraphProvider.class) && !SparkHadoopGraphProvider.class.getCanonicalName().equals(System.getProperty(PREVIOUS_SPARK_PROVIDER, null))) {
            Spark.close();
            HadoopPools.close();
            KryoShimServiceLoader.close();
            System.setProperty(PREVIOUS_SPARK_PROVIDER, SparkHadoopGraphProvider.class.getCanonicalName());
        }

        final Map<String,Object> config = new HashMap<String, Object>() {{
            put(Graph.GRAPH, HadoopGraph.class.getName());
            put(Constants.GREMLIN_HADOOP_GRAPH_READER, graphSONInput ? GraphSONInputFormat.class.getCanonicalName() : GryoInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, getWorkingDirectory());
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);

            put(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);  // this makes the test suite go really fast
        }};

        // toy graph inputRDD does not have corresponding outputRDD so where jobs chain, it fails (failing makes sense)
        if (null != loadGraphWith &&
                !test.equals(ProgramTest.Traversals.class) &&
                !test.equals(PageRankTest.Traversals.class) &&
                !test.equals(PeerPressureTest.Traversals.class) &&
                !test.equals(FileSystemStorageCheck.class) &&
                !testMethodName.equals("shouldSupportJobChaining") &&  // GraphComputerTest.shouldSupportJobChaining
                RANDOM.nextBoolean()) {
            config.put(Constants.GREMLIN_HADOOP_GRAPH_READER, ToyGraphInputRDD.class.getCanonicalName());
        }

        // tests persisted RDDs
        if (test.equals(SparkContextStorageCheck.class)) {
            config.put(Constants.GREMLIN_HADOOP_GRAPH_READER, ToyGraphInputRDD.class.getCanonicalName());
            config.put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        }

        config.put(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        config.put(SparkLauncher.SPARK_MASTER, "local[4]");
        config.put(Constants.SPARK_SERIALIZER, KryoSerializer.class.getCanonicalName());
        config.put(Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getCanonicalName());
        config.put(Constants.SPARK_KRYO_REGISTRATION_REQUIRED, true);
        return config;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return RANDOM.nextBoolean() ?
                graph.traversal().withComputer(Computer.compute(SparkGraphComputer.class).workers(RANDOM.nextInt(3) + 1)) :
                graph.traversal().withComputer();
    }

    @Override
    public GraphComputer getGraphComputer(final Graph graph) {
        return RANDOM.nextBoolean() ?
                graph.compute().workers(RANDOM.nextInt(3) + 1) :
                graph.compute(SparkGraphComputer.class);
    }
}