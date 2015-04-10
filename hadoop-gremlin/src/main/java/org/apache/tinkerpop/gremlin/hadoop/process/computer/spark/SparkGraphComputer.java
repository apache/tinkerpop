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
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.ViewIncomingPayload;
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
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.HashSet;
import java.util.Optional;
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

    private Optional<ResultGraph> resultGraph = Optional.empty();
    private Optional<Persist> persist = Optional.empty();

    public SparkGraphComputer(final HadoopGraph hadoopGraph) {
        this.hadoopGraph = hadoopGraph;
    }

    @Override
    public GraphComputer isolation(final Isolation isolation) {
        if (!isolation.equals(Isolation.BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);  // todo: dirty_bsp is when there is no doNothing() call at the end of the round?
        return this;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = Optional.of(resultGraph);
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = Optional.of(persist);
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

        // determine persistence and result graph options
        if (!this.persist.isPresent())
            this.persist = Optional.of(null == this.vertexProgram ? Persist.NOTHING : this.vertexProgram.getPreferredPersist());
        if (!this.resultGraph.isPresent())
            this.resultGraph = Optional.of(null == this.vertexProgram ? ResultGraph.ORIGINAL : this.vertexProgram.getPreferredResultGraph());
        if (this.resultGraph.get().equals(ResultGraph.ORIGINAL))
            if (!this.persist.get().equals(Persist.NOTHING))
                throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph.get(), this.persist.get());

        // apache and hadoop configurations that are used throughout
        final org.apache.commons.configuration.Configuration apacheConfiguration = new HadoopConfiguration(this.hadoopGraph.configuration());
        apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, this.persist.get().equals(Persist.EDGES));
        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(apacheConfiguration);

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
                        SparkGraphComputer.loadJars(sparkContext, hadoopConfiguration);
                        // create a message-passing friendly rdd from the hadoop input format
                        final JavaPairRDD<Object, VertexWritable> graphRDD = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                NullWritable.class,
                                VertexWritable.class)
                                .mapToPair(tuple -> new Tuple2<>(tuple._2().get().id(), new VertexWritable(tuple._2().get())))
                                .reduceByKey((a, b) -> a) // TODO: why is this necessary?
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
                            // drop all edges and messages in the graphRDD as they are no longer needed for the map reduce jobs
                            final JavaPairRDD<Object, VertexWritable> mapReduceGraphRDD = SparkExecutor.prepareGraphRDDForMapReduce(graphRDD, viewIncomingRDD).setName("mapReduceGraphRDD").cache();
                            for (final MapReduce mapReduce : this.mapReducers) {
                                // execute the map reduce job
                                final HadoopConfiguration newApacheConfiguration = new HadoopConfiguration(apacheConfiguration);
                                mapReduce.storeState(newApacheConfiguration);
                                // map
                                final JavaPairRDD mapRDD = SparkExecutor.executeMap((JavaPairRDD) mapReduceGraphRDD, mapReduce, newApacheConfiguration).setName("mapRDD");
                                // combine TODO? is this really needed
                                // reduce
                                final JavaPairRDD reduceRDD = (mapReduce.doStage(MapReduce.Stage.REDUCE)) ? SparkExecutor.executeReduce(mapRDD, mapReduce, newApacheConfiguration).setName("reduceRDD") : null;
                                // write the map reduce output back to disk (memory)
                                SparkExecutor.saveMapReduceRDD(null == reduceRDD ? mapRDD : reduceRDD, mapReduce, finalMemory, hadoopConfiguration);
                            }
                        }
                        // update runtime and return the newly computed graph
                        finalMemory.setRuntime(System.currentTimeMillis() - startTime);
                        return new DefaultComputerResult(HadoopHelper.getOutputGraph(this.hadoopGraph, this.resultGraph.get(), this.persist.get()), finalMemory.asImmutable());
                    }
                }
        );
    }

    /////////////////

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

            public boolean supportsVertexAddition() {
                return false;
            }

            public boolean supportsVertexRemoval() {
                return false;
            }

            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            public boolean supportsEdgeAddition() {
                return false;
            }

            public boolean supportsEdgeRemoval() {
                return false;
            }

            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            public boolean supportsEdgePropertyRemoval() {
                return false;
            }

            public boolean supportsIsolation(final Isolation isolation) {
                return isolation.equals(Isolation.BSP);
            }

            public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
                return persist.equals(Persist.NOTHING) || resultGraph.equals(ResultGraph.NEW);
            }

            public boolean supportsDirectObjects() {
                return false;
            }
        };
    }
}
