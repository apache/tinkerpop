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

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.util.SparkHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.HadoopHelper;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultMemory;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkGraphComputer implements GraphComputer {

    public static final Logger LOGGER = LoggerFactory.getLogger(SparkGraphComputer.class);

    protected final SparkConf configuration = new SparkConf();
    protected final HadoopGraph hadoopGraph;
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private VertexProgram vertexProgram;

    public SparkGraphComputer(final HadoopGraph hadoopGraph) {
        this.hadoopGraph = hadoopGraph;
    }

    @Override
    public GraphComputer isolation(final Isolation isolation) {
        if (!isolation.equals(Isolation.BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    @Override
    public Future<ComputerResult> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        // apache and hadoop configurations that are used throughout
        final org.apache.commons.configuration.Configuration apacheConfiguration = this.hadoopGraph.configuration();
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.hadoopGraph.configuration());

        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
                    final long startTime = System.currentTimeMillis();
                    SparkMemory memory = null;
                    SparkHelper.deleteOutputDirectory(hadoopConfiguration);
                    ////////////////////////////////
                    // process the vertex program //
                    ////////////////////////////////
                    if (null != this.vertexProgram) {
                        // set up the spark job
                        final SparkConf sparkConfiguration = new SparkConf();
                        sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + this.vertexProgram);
                        hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
                        if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class)))
                            hadoopConfiguration.set("mapred.input.dir", hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION)); // necessary for Spark and newAPIHadoopRDD
                        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
                        SparkGraphComputer.loadJars(sparkContext, hadoopConfiguration);
                        ///
                        try {
                            // create a message-passing friendly rdd from the hadoop input format
                            JavaPairRDD<Object, SparkMessenger<Object>> graphRDD = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                    (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                    NullWritable.class,
                                    VertexWritable.class)
                                    .mapToPair(tuple -> new Tuple2<>(tuple._2().get().id(), new SparkMessenger<>(new SparkVertex((TinkerVertex) tuple._2().get()))));

                            // set up the vertex program and wire up configurations
                            memory = new SparkMemory(this.vertexProgram, this.mapReducers, sparkContext);
                            this.vertexProgram.setup(memory);
                            final SerializableConfiguration vertexProgramConfiguration = new SerializableConfiguration();
                            this.vertexProgram.storeState(vertexProgramConfiguration);
                            ConfUtil.mergeApacheIntoHadoopConfiguration(vertexProgramConfiguration, hadoopConfiguration);
                            ConfigurationUtils.copy(vertexProgramConfiguration, apacheConfiguration);

                            // execute the vertex program
                            do {
                                graphRDD = SparkHelper.executeStep(graphRDD, this.vertexProgram, memory, vertexProgramConfiguration);
                                graphRDD.foreachPartition(iterator -> doNothing()); // i think this is a fast way to execute the rdd
                                graphRDD.cache(); // TODO: learn about persistence and caching
                                memory.incrIteration();
                            } while (!this.vertexProgram.terminate(memory));

                            // write the output graph back to disk
                            SparkHelper.saveVertexProgramRDD(graphRDD, hadoopConfiguration);
                        } finally {
                            // must close the context or bad things happen
                            sparkContext.close();
                        }
                        sparkContext.close(); // why not try again todo
                    }

                    //////////////////////////////
                    // process the map reducers //
                    //////////////////////////////
                    final Memory.Admin finalMemory = null == memory ? new DefaultMemory() : new DefaultMemory(memory);
                    for (final MapReduce mapReduce : this.mapReducers) {
                        // set up the spark job
                        final SparkConf sparkConfiguration = new SparkConf();
                        sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + mapReduce);
                        hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
                        if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class)))
                            hadoopConfiguration.set("mapred.input.dir", hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G);
                        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
                        SparkGraphComputer.loadJars(sparkContext, hadoopConfiguration);
                        // execute the map reduce job
                        try {
                            final JavaPairRDD<NullWritable, VertexWritable> hadoopGraphRDD = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                    (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                    NullWritable.class,
                                    VertexWritable.class);

                            final SerializableConfiguration newApacheConfiguration = new SerializableConfiguration(apacheConfiguration);
                            mapReduce.storeState(newApacheConfiguration);
                            // map
                            final JavaPairRDD mapRDD = SparkHelper.executeMap(hadoopGraphRDD, mapReduce, newApacheConfiguration);
                            // combine todo
                            // reduce
                            final JavaPairRDD reduceRDD = (mapReduce.doStage(MapReduce.Stage.REDUCE)) ? SparkHelper.executeReduce(mapRDD, mapReduce, newApacheConfiguration) : null;

                            // write the map reduce output back to disk (memory)
                            SparkHelper.saveMapReduceRDD(null == reduceRDD ? mapRDD : reduceRDD, mapReduce, finalMemory, hadoopConfiguration);
                        } finally {
                            // must close the context or bad things happen
                            sparkContext.close();
                        }
                        sparkContext.close(); // why not try again todo
                    }

                    // update runtime and return the newly computed graph
                    finalMemory.setRuntime(System.currentTimeMillis() - startTime);
                    return new DefaultComputerResult(HadoopHelper.getOutputGraph(this.hadoopGraph), finalMemory.asImmutable());
                }
        );
    }

    /////////////////

    private static final void doNothing() {
        // a cheap action
    }

    private static void loadJars(final JavaSparkContext sparkContext, final Configuration hadoopConfiguration) {
        if (hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLocalLibs = System.getenv(Constants.HADOOP_GREMLIN_LIBS);
            if (null == hadoopGremlinLocalLibs)
                LOGGER.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final String[] paths = hadoopGremlinLocalLibs.split(":");
                for (final String path : paths) {
                    final File file = new File(path);
                    if (file.exists())
                        Stream.of(file.listFiles()).filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> sparkContext.addJar(f.getAbsolutePath()));
                    else
                        LOGGER.warn(path + " does not reference a valid directory -- proceeding regardless");
                }
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new SparkGraphComputer(HadoopGraph.open(configuration)).program(VertexProgram.createVertexProgram(configuration)).submit().get();
    }

    @Override
    public Features features() {
        return new Features() {
            @Override
            public boolean supportsNonSerializableObjects() {
                return true;  // TODO
            }
        };
    }
}
