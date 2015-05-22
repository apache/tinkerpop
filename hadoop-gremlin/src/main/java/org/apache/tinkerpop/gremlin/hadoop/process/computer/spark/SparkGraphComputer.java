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
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.HadoopHelper;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import scala.Tuple2;

import java.io.File;
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
    public Future<ComputerResult> submit() {
        super.validateStatePriorToExecution();
        // apache and hadoop configurations that are used throughout
        final org.apache.commons.configuration.Configuration apacheConfiguration = new HadoopConfiguration(this.hadoopGraph.configuration());
        apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, this.persist.get().equals(Persist.EDGES));
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(apacheConfiguration);
        // create the completable future
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
                    final long startTime = System.currentTimeMillis();
                    SparkMemory memory = null;
                    SparkExecutor.deleteOutputLocation(hadoopConfiguration);

                    // wire up a spark context
                    final SparkConf sparkConfiguration = new SparkConf();
                    sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + (null == this.vertexProgram ? "No VertexProgram" : this.vertexProgram) + "[" + this.mapReducers + "]");
                    /*final List<Class> classes = new ArrayList<>();
                    classes.addAll(IOClasses.getGryoClasses(GryoMapper.build().create()));
                    classes.addAll(IOClasses.getSharedHadoopClasses());
                    classes.add(ViewPayload.class);
                    classes.add(MessagePayload.class);
                    classes.add(ViewIncomingPayload.class);
                    classes.add(ViewOutgoingPayload.class);
                    sparkConfiguration.registerKryoClasses(classes.toArray(new Class[classes.size()]));*/ // TODO: fix for user submitted jars in Spark 1.3.0

                    hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
                    if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class)))
                        hadoopConfiguration.set(Constants.MAPRED_INPUT_DIR, SparkExecutor.getInputLocation(hadoopConfiguration)); // necessary for Spark and newAPIHadoopRDD
                    // execute the vertex program and map reducers and if there is a failure, auto-close the spark context
                    try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration)) {
                        // add the project jars to the cluster
                        this.loadJars(sparkContext, hadoopConfiguration);
                        // create a message-passing friendly rdd from the hadoop input format
                        final JavaPairRDD<Object, VertexWritable> graphRDD = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                NullWritable.class,
                                VertexWritable.class)
                                .mapToPair(tuple -> new Tuple2<>(tuple._2().get().id(), new VertexWritable(tuple._2().get())))
                                .reduceByKey((a, b) -> a) // if this is not done, then the graph is partitioned and you can have duplicate vertices
                                .setName("graphRDD")
                                .cache(); // partition the graph across the cluster
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
                            // write the output graph back to disk
                            if (!this.persist.get().equals(Persist.NOTHING))
                                SparkExecutor.saveGraphRDD(graphRDD, hadoopConfiguration);
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
                        //SparkExecutor.saveMemory(finalMemory, hadoopConfiguration);
                        return new DefaultComputerResult(HadoopHelper.getOutputGraph(this.hadoopGraph, this.resultGraph.get(), this.persist.get()), finalMemory.asImmutable());
                    }
                }
        );
    }

    /////////////////

    private void loadJars(final JavaSparkContext sparkContext, final Configuration hadoopConfiguration) {
        if (hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLocalLibs = System.getenv(Constants.HADOOP_GREMLIN_LIBS);
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
