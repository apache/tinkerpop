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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.ComputerSubmissionHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkGraphComputer extends AbstractHadoopGraphComputer {

    private final org.apache.commons.configuration.Configuration sparkConfiguration;
    private boolean workersSet = false;

    public SparkGraphComputer(final HadoopGraph hadoopGraph) {
        super(hadoopGraph);
        this.sparkConfiguration = new HadoopConfiguration();
        ConfigurationUtils.copy(this.hadoopGraph.configuration(), this.sparkConfiguration);
    }

    @Override
    public GraphComputer workers(final int workers) {
        super.workers(workers);
        if (this.sparkConfiguration.getString(SparkLauncher.SPARK_MASTER).startsWith("local")) {
            this.sparkConfiguration.setProperty(SparkLauncher.SPARK_MASTER, "local[" + this.workers + "]");
        }
        this.workersSet = true;
        return this;
    }

    @Override
    public GraphComputer configure(final String key, final Object value) {
        this.sparkConfiguration.setProperty(key, value);
        return this;
    }

    @Override
    protected void validateStatePriorToExecution() {
        super.validateStatePriorToExecution();
        if (this.sparkConfiguration.containsKey(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD) && this.sparkConfiguration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT))
            this.logger.warn("Both " + Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD + " and " + Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT + " were specified, ignoring " + Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT);
        if (this.sparkConfiguration.containsKey(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD) && this.sparkConfiguration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT))
            this.logger.warn("Both " + Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD + " and " + Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT + " were specified, ignoring " + Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT);
    }

    @Override
    public Future<ComputerResult> submit() {
        this.validateStatePriorToExecution();

        return ComputerSubmissionHelper.runWithBackgroundThread(this::submitWithExecutor, "SparkSubmitter");
    }

    private Future<ComputerResult> submitWithExecutor(Executor exec) {
        // apache and hadoop configurations that are used throughout the graph computer computation
        final org.apache.commons.configuration.Configuration apacheConfiguration = new HadoopConfiguration(this.sparkConfiguration);
        apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, this.persist.equals(GraphComputer.Persist.EDGES));
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(apacheConfiguration);
        if (hadoopConfiguration.get(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, null) == null && // if an InputRDD is specified, then ignore InputFormat
                hadoopConfiguration.get(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, null) != null &&
                FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class))) {
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
            try {
                if (null != outputLocation && FileSystem.get(hadoopConfiguration).exists(new Path(outputLocation)))
                    FileSystem.get(hadoopConfiguration).delete(new Path(outputLocation), true);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            // wire up a spark context
            final SparkConf sparkConfiguration = new SparkConf();
            sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + (null == this.vertexProgram ? "No VertexProgram" : this.vertexProgram) + "[" + this.mapReducers + "]");

            // create the spark configuration from the graph computer configuration
            hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
            // execute the vertex program and map reducers and if there is a failure, auto-close the spark context
            try {
                final JavaSparkContext sparkContext = new JavaSparkContext(SparkContext.getOrCreate(sparkConfiguration));
                Spark.create(sparkContext.sc()); // this is the context RDD holder that prevents GC
                updateLocalConfiguration(sparkContext, sparkConfiguration);
                // add the project jars to the cluster
                this.loadJars(sparkContext, hadoopConfiguration);
                // create a message-passing friendly rdd from the input rdd
                JavaPairRDD<Object, VertexWritable> graphRDD;
                try {
                    graphRDD = hadoopConfiguration.getClass(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, InputFormatRDD.class, InputRDD.class)
                            .newInstance()
                            .readGraphRDD(apacheConfiguration, sparkContext);
                    if (this.workersSet && graphRDD.partitions().size() > this.workers) // ensures that the graphRDD does not have more partitions than workers
                        graphRDD = graphRDD.coalesce(this.workers);
                    graphRDD = graphRDD.cache();
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
                    final String[] elementComputeKeys = this.vertexProgram == null ? new String[0] : this.vertexProgram.getElementComputeKeys().toArray(new String[this.vertexProgram.getElementComputeKeys().size()]);
                    graphRDD = SparkExecutor.prepareFinalGraphRDD(graphRDD, viewIncomingRDD, elementComputeKeys);
                    if ((hadoopConfiguration.get(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, null) != null ||
                            hadoopConfiguration.get(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null) != null) &&
                            !this.persist.equals(GraphComputer.Persist.NOTHING)) {
                        try {
                            hadoopConfiguration.getClass(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, OutputFormatRDD.class, OutputRDD.class)
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
                    final JavaPairRDD<Object, VertexWritable> mapReduceGraphRDD = graphRDD.mapValues(vertexWritable -> {
                        vertexWritable.get().dropEdges();
                        return vertexWritable;
                    }).cache();  // drop all the edges of the graph as they are not used in mapReduce processing
                    for (final MapReduce mapReduce : this.mapReducers) {
                        // execute the map reduce job
                        final HadoopConfiguration newApacheConfiguration = new HadoopConfiguration(apacheConfiguration);
                        mapReduce.storeState(newApacheConfiguration);
                        // map
                        final JavaPairRDD mapRDD = SparkExecutor.executeMap((JavaPairRDD) mapReduceGraphRDD, mapReduce, newApacheConfiguration);
                        // combine TODO: is this really needed?
                        // reduce
                        final JavaPairRDD reduceRDD = (mapReduce.doStage(MapReduce.Stage.REDUCE)) ? SparkExecutor.executeReduce(mapRDD, mapReduce, newApacheConfiguration) : null;
                        // write the map reduce output back to disk (memory)
                        SparkExecutor.saveMapReduceRDD(null == reduceRDD ? mapRDD : reduceRDD, mapReduce, finalMemory, hadoopConfiguration);
                    }
                    mapReduceGraphRDD.unpersist();
                }

                // unpersist the graphRDD if it will no longer be used
                if (!PersistedOutputRDD.class.equals(hadoopConfiguration.getClass(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null)) || this.persist.equals(GraphComputer.Persist.NOTHING)) {
                    graphRDD.unpersist();
                    Spark.removeRDD(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)); // delete persisted output rdd if it exists
                }
                // update runtime and return the newly computed graph
                finalMemory.setRuntime(System.currentTimeMillis() - startTime);
                return new DefaultComputerResult(InputOutputHelper.getOutputGraph(apacheConfiguration, this.resultGraph, this.persist), finalMemory.asImmutable());
            } finally {
                if (!apacheConfiguration.getBoolean(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false))
                    Spark.close();
            }
        }, exec);
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
                        Stream.of(file.listFiles()).filter(f -> f.getName().contains(Constants.DOT_JAR)).forEach(f -> sparkContext.addJar(f.getAbsolutePath()));
                    else
                        this.logger.warn(path + " does not reference a valid directory -- proceeding regardless");
                }
            }
        }
    }

    /**
     * When using a persistent context the running Context's configuration will override a passed
     * in configuration. Spark allows us to override these inherited properties via
     * SparkContext.setLocalProperty
     */
    private void updateLocalConfiguration(final JavaSparkContext sparkContext, final SparkConf sparkConfiguration) {
        /*
         * While we could enumerate over the entire SparkConfiguration and copy into the Thread
         * Local properties of the Spark Context this could cause adverse effects with future
         * versions of Spark. Since the api for setting multiple local properties at once is
         * restricted as private, we will only set those properties we know can effect SparkGraphComputer
         * Execution rather than applying the entire configuration.
         */
        final String[] validPropertyNames = {
                "spark.job.description",
                "spark.jobGroup.id",
                "spark.job.interruptOnCancel",
                "spark.scheduler.pool"
        };

        for (String propertyName : validPropertyNames) {
            if (sparkConfiguration.contains(propertyName)) {
                String propertyValue = sparkConfiguration.get(propertyName);
                this.logger.info("Setting Thread Local SparkContext Property - "
                        + propertyName + " : " + propertyValue);

                sparkContext.setLocalProperty(propertyName, sparkConfiguration.get(propertyName));
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new SparkGraphComputer(HadoopGraph.open(configuration)).program(VertexProgram.createVertexProgram(HadoopGraph.open(configuration), configuration)).submit().get();
    }
}
