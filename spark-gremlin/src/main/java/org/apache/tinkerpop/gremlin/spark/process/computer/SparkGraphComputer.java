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

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
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
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.process.computer.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.process.computer.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.process.computer.io.OutputRDD;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkGraphComputer extends AbstractHadoopGraphComputer {

    public SparkGraphComputer(final HadoopGraph hadoopGraph) {
        super(hadoopGraph);
    }

    @Override
    public GraphComputer workers(final int workers) {
        super.workers(workers);
        if (this.hadoopGraph.configuration().getString("spark.master").startsWith("local")) {
            this.hadoopGraph.configuration().setProperty("spark.master", "local[" + this.workers + "]");
        }
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        super.validateStatePriorToExecution();
        // apache and hadoop configurations that are used throughout the graph computer computation
        final org.apache.commons.configuration.Configuration apacheConfiguration = new HadoopConfiguration(this.hadoopGraph.configuration());
        apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, this.persist.equals(GraphComputer.Persist.EDGES));
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(apacheConfiguration);
        if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class))) {
            try {
                final String inputLocation = FileSystem.get(hadoopConfiguration).getFileStatus(new Path(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION))).getPath().toString();
                apacheConfiguration.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputLocation);
                hadoopConfiguration.set(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputLocation);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        // create the completable future
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            final long startTime = System.currentTimeMillis();
            SparkMemory memory = null;
            // delete output location
            final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
            if (null != outputLocation) {
                try {
                    FileSystem.get(hadoopConfiguration).delete(new Path(outputLocation), true);
                } catch (final IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
            // wire up a spark context
            final SparkConf sparkConfiguration = new SparkConf();
            sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + (null == this.vertexProgram ? "No VertexProgram" : this.vertexProgram) + "[" + this.mapReducers + "]");

            // create the spark configuration from the graph computer configuration
            hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
            // execute the vertex program and map reducers and if there is a failure, auto-close the spark context
            try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration)) {
                // add the project jars to the cluster
                this.loadJars(sparkContext, hadoopConfiguration);
                // create a message-passing friendly rdd from the input rdd
                final JavaPairRDD<Object, VertexWritable> graphRDD;
                try {
                    graphRDD = hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_RDD, InputFormatRDD.class, InputRDD.class)
                            .newInstance()
                            .readGraphRDD(apacheConfiguration, sparkContext)
                            .setName("graphRDD")
                            .cache();
                } catch (final InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
                JavaPairRDD<Object, ViewIncomingPayload<Object>> viewIncomingRDD = null;

                ////////////////////////////////
                // process the vertex program //
                ////////////////////////////////
                if (null != this.vertexProgram) {
                    // set up the vertex program and wire up configurations
                    memory = new SparkMemory(this.vertexProgram, this.mapReducers, sparkContext);
                    this.vertexProgram.setup(memory);
                    memory.broadcastMemory(sparkContext);
                    final HadoopConfiguration vertexProgramConfiguration = new HadoopConfiguration();
                    this.vertexProgram.storeState(vertexProgramConfiguration);
                    ConfigurationUtils.copy(vertexProgramConfiguration, apacheConfiguration);
                    ConfUtil.mergeApacheIntoHadoopConfiguration(vertexProgramConfiguration, hadoopConfiguration);
                    // execute the vertex program
                    while (true) {
                        memory.setInTask(true);
                        viewIncomingRDD = SparkExecutor.executeVertexProgramIteration(graphRDD, viewIncomingRDD, memory, vertexProgramConfiguration);
                        memory.setInTask(false);
                        if (this.vertexProgram.terminate(memory))
                            break;
                        else {
                            memory.incrIteration();
                            memory.broadcastMemory(sparkContext);
                        }
                    }
                    // write the graph rdd using the output rdd
                    if (!this.persist.equals(GraphComputer.Persist.NOTHING)) {
                        try {
                            hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_RDD, OutputFormatRDD.class, OutputRDD.class)
                                    .newInstance()
                                    .writeGraphRDD(apacheConfiguration, graphRDD);
                        } catch (final InstantiationException | IllegalAccessException e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    }
                }

                final Memory.Admin finalMemory = null == memory ? new MapMemory() : new MapMemory(memory);

                //////////////////////////////
                // process the map reducers //
                //////////////////////////////
                if (!this.mapReducers.isEmpty()) {
                    final String[] elementComputeKeys = this.vertexProgram == null ? new String[0] : this.vertexProgram.getElementComputeKeys().toArray(new String[this.vertexProgram.getElementComputeKeys().size()]);
                    final JavaPairRDD<Object, VertexWritable> mapReduceGraphRDD = SparkExecutor.prepareGraphRDDForMapReduce(graphRDD, viewIncomingRDD, elementComputeKeys).setName("mapReduceGraphRDD").cache();
                    for (final MapReduce mapReduce : this.mapReducers) {
                        // execute the map reduce job
                        final HadoopConfiguration newApacheConfiguration = new HadoopConfiguration(apacheConfiguration);
                        mapReduce.storeState(newApacheConfiguration);
                        // map
                        final JavaPairRDD mapRDD = SparkExecutor.executeMap((JavaPairRDD) mapReduceGraphRDD, mapReduce, newApacheConfiguration).setName("mapRDD");
                        // combine TODO: is this really needed
                        // reduce
                        final JavaPairRDD reduceRDD = (mapReduce.doStage(MapReduce.Stage.REDUCE)) ? SparkExecutor.executeReduce(mapRDD, mapReduce, newApacheConfiguration).setName("reduceRDD") : null;
                        // write the map reduce output back to disk (memory)
                        SparkExecutor.saveMapReduceRDD(null == reduceRDD ? mapRDD : reduceRDD, mapReduce, finalMemory, hadoopConfiguration);
                    }
                }
                // update runtime and return the newly computed graph
                finalMemory.setRuntime(System.currentTimeMillis() - startTime);
                return new DefaultComputerResult(HadoopHelper.getOutputGraph(this.hadoopGraph, this.resultGraph, this.persist), finalMemory.asImmutable());
            }
        });
    }

    /////////////////

    private void loadJars(final JavaSparkContext sparkContext, final Configuration hadoopConfiguration) {
        if (hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLocalLibs = null == System.getProperty(Constants.HADOOP_GREMLIN_LIBS) ? System.getenv(Constants.HADOOP_GREMLIN_LIBS) : System.getProperty(Constants.HADOOP_GREMLIN_LIBS);
            if (null == hadoopGremlinLocalLibs)
                this.logger.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final String[] paths = hadoopGremlinLocalLibs.split(":");
                for (final String path : paths) {
                    final File file = new File(path);
                    if (file.exists())
                        Stream.of(file.listFiles()).filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> sparkContext.addJar(f.getAbsolutePath()));
                    else
                        this.logger.warn(path + " does not reference a valid directory -- proceeding regardless");
                }
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new SparkGraphComputer(HadoopGraph.open(configuration)).program(VertexProgram.createVertexProgram(HadoopGraph.open(configuration), configuration)).submit().get();
    }
}
