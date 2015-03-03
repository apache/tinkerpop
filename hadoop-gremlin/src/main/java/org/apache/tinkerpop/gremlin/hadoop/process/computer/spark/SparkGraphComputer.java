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

import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.HadoopHelper;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkGraphComputer implements GraphComputer {

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
        if (null != this.vertexProgram)
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        final Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.hadoopGraph.configuration());
        final SparkMemory memory = new SparkMemory(Collections.emptySet());

        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
                    final long startTime = System.currentTimeMillis();
                    // load the graph
                    if (null != this.vertexProgram) {
                        final SparkConf sparkConfiguration = new SparkConf();
                        sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + this.vertexProgram);
                        hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
                        if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class)))
                            hadoopConfiguration.set("mapred.input.dir", hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION));

                        // set up the input format
                        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
                        SparkGraphComputer.loadJars(sparkContext, hadoopConfiguration);
                        final JavaPairRDD<NullWritable, VertexWritable> rdd = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                NullWritable.class,
                                VertexWritable.class);
                        final JavaPairRDD<Object, SparkMessenger<Object>> rdd2 = rdd.mapToPair(tuple -> new Tuple2<>(tuple._2().get().id(), new SparkMessenger<>(new SparkVertex((TinkerVertex) tuple._2().get()), new ArrayList<>())));
                        GraphComputerRDD<Object> g = GraphComputerRDD.of(rdd2);

                        // set up the vertex program
                        this.vertexProgram.setup(memory);
                        final org.apache.commons.configuration.Configuration vertexProgramConfiguration = new SerializableConfiguration();
                        this.vertexProgram.storeState(vertexProgramConfiguration);

                        // execute the vertex program
                        while (true) {
                            g = g.execute(vertexProgramConfiguration, memory);
                            g.foreachPartition(iterator -> doNothing());
                            memory.incrIteration();
                            if (this.vertexProgram.terminate(memory))
                                break;
                        }
                        // write the output graph back to disk
                        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
                        if (null != outputLocation) {
                            try {
                                FileSystem.get(hadoopConfiguration).delete(new Path(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)), true);
                            } catch (final IOException e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                            // map back to a <nullwritable,vertexwritable> stream for output
                            g.mapToPair(tuple -> new Tuple2<>(NullWritable.get(), new VertexWritable<>(tuple._2().vertex)))
                                    .saveAsNewAPIHadoopFile(outputLocation + "/" + Constants.SYSTEM_G,
                                            NullWritable.class,
                                            VertexWritable.class,
                                            (Class<OutputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class));
                        }
                        sparkContext.close();
                    }

                    // execute mapreduce jobs
                    for (final MapReduce mapReduce : this.mapReducers) {
                        // set up the map reduce job
                        final org.apache.commons.configuration.Configuration mapReduceConfiguration = new SerializableConfiguration();
                        mapReduce.storeState(mapReduceConfiguration);

                        // set up spark job
                        final SparkConf sparkConfiguration = new SparkConf();
                        sparkConfiguration.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX + mapReduce);
                        hadoopConfiguration.forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
                        if (FileInputFormat.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class)))
                            hadoopConfiguration.set("mapred.input.dir", hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G);
                        // set up the input format
                        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
                        SparkGraphComputer.loadJars(sparkContext, hadoopConfiguration);
                        final JavaPairRDD<NullWritable, VertexWritable> g = sparkContext.newAPIHadoopRDD(hadoopConfiguration,
                                (Class<InputFormat<NullWritable, VertexWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class),
                                NullWritable.class,
                                VertexWritable.class);

                        // map
                        JavaPairRDD<?, ?> mapRDD = g.flatMapToPair(tuple -> {
                            final MapReduce m = MapReduce.createMapReduce(mapReduceConfiguration);
                            final SparkMapEmitter mapEmitter = new SparkMapEmitter();
                            m.map(tuple._2().get(), mapEmitter);
                            return mapEmitter.getEmissions();
                        });
                        if (mapReduce.getMapKeySort().isPresent())
                            mapRDD = mapRDD.sortByKey((Comparator) mapReduce.getMapKeySort().get());
                        // todo: combine
                        // reduce
                        JavaPairRDD<?, ?> reduceRDD = null;
                        if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                            reduceRDD = mapRDD.groupByKey().flatMapToPair(tuple -> {
                                final MapReduce m = MapReduce.createMapReduce(mapReduceConfiguration);
                                final SparkReduceEmitter reduceEmitter = new SparkReduceEmitter();
                                m.reduce(tuple._1(), tuple._2().iterator(), reduceEmitter);
                                return reduceEmitter.getEmissions();
                            });
                            if (mapReduce.getReduceKeySort().isPresent())
                                reduceRDD = reduceRDD.sortByKey((Comparator) mapReduce.getReduceKeySort().get());
                        }
                        // write the output graph back to disk
                        final String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
                        if (null != outputLocation) {
                            // map back to a <nullwritable,vertexwritable> stream for output
                            ((null == reduceRDD) ? mapRDD : reduceRDD).mapToPair(tuple -> new Tuple2<>(new ObjectWritable<>(tuple._1()), new ObjectWritable<>(tuple._2()))).saveAsNewAPIHadoopFile(outputLocation + "/" + mapReduce.getMemoryKey(),
                                    ObjectWritable.class,
                                    ObjectWritable.class,
                                    (Class<OutputFormat<ObjectWritable, ObjectWritable>>) hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, OutputFormat.class));
                            // if its not a SequenceFile there is no certain way to convert to necessary Java objects.
                            // to get results you have to look through HDFS directory structure. Oh the horror.
                            try {
                                if (hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
                                    mapReduce.addResultToMemory(memory, new ObjectWritableIterator(hadoopConfiguration, new Path(outputLocation + "/" + mapReduce.getMemoryKey())));
                                else
                                    HadoopGraph.LOGGER.warn(Constants.SEQUENCE_WARNING);
                            } catch (final IOException e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                        }
                        sparkContext.close();
                    }

                    // update runtime and return the newly computed graph
                    memory.setRuntime(System.currentTimeMillis() - startTime);
                    memory.complete();
                    return new DefaultComputerResult(HadoopHelper.getOutputGraph(this.hadoopGraph), memory.asImmutable());
                }
        );
    }

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

    /////////////////

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration("/Users/marko/software/tinkerpop/tinkerpop3/hadoop-gremlin/conf/spark-kryo.properties");
        // TODO: final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        final HadoopGraph graph = HadoopGraph.open(configuration);
        final ComputerResult result = new SparkGraphComputer(graph).program(VertexProgram.createVertexProgram(configuration)).mapReduce(PageRankMapReduce.build().create()).submit().get();
        // TODO: remove everything below
        System.out.println(result);
        result.memory().<Iterator>get(PageRankMapReduce.DEFAULT_MEMORY_KEY).forEachRemaining(System.out::println);
        //result.graph().configuration().getKeys().forEachRemaining(key -> System.out.println(key + "-->" + result.graph().configuration().getString(key)));
        result.graph().V().valueMap().forEachRemaining(System.out::println);
    }


}
