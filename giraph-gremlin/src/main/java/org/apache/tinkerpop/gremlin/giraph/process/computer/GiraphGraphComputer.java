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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tinkerpop.gremlin.giraph.structure.io.GiraphVertexInputFormat;
import org.apache.tinkerpop.gremlin.giraph.structure.io.GiraphVertexOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.ComputerSubmissionHelper;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.MapReduceHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.util.Gremlin;

import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphGraphComputer extends AbstractHadoopGraphComputer implements GraphComputer, Tool {

    protected GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    private MapMemory memory = new MapMemory();
    private boolean useWorkerThreadsInConfiguration;

    public GiraphGraphComputer(final HadoopGraph hadoopGraph) {
        super(hadoopGraph);
        final Configuration configuration = hadoopGraph.configuration();
        configuration.getKeys().forEachRemaining(key -> this.giraphConfiguration.set(key, configuration.getProperty(key).toString()));
        this.giraphConfiguration.setMasterComputeClass(GiraphMemory.class);
        this.giraphConfiguration.setVertexClass(GiraphVertex.class);
        this.giraphConfiguration.setComputationClass(GiraphComputation.class);
        this.giraphConfiguration.setWorkerContextClass(GiraphWorkerContext.class);
        this.giraphConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        this.giraphConfiguration.setClass(GiraphConstants.VERTEX_ID_CLASS.getKey(), ObjectWritable.class, ObjectWritable.class);
        this.giraphConfiguration.setClass(GiraphConstants.VERTEX_VALUE_CLASS.getKey(), VertexWritable.class, VertexWritable.class);
        this.giraphConfiguration.setBoolean(GiraphConstants.STATIC_GRAPH.getKey(), true);
        this.giraphConfiguration.setVertexInputFormatClass(GiraphVertexInputFormat.class);
        this.giraphConfiguration.setVertexOutputFormatClass(GiraphVertexOutputFormat.class);
        this.useWorkerThreadsInConfiguration = this.giraphConfiguration.getInt(GiraphConstants.MAX_WORKERS, -666) != -666 || this.giraphConfiguration.getInt(GiraphConstants.NUM_COMPUTE_THREADS.getKey(), -666) != -666;
    }

    @Override
    public GraphComputer workers(final int workers) {
        this.useWorkerThreadsInConfiguration = false;
        return super.workers(workers);
    }

    @Override
    public GraphComputer configure(final String key, final Object value) {
        this.giraphConfiguration.set(key, value.toString());
        this.useWorkerThreadsInConfiguration = this.giraphConfiguration.getInt(GiraphConstants.MAX_WORKERS, -666) != -666 || this.giraphConfiguration.getInt(GiraphConstants.NUM_COMPUTE_THREADS.getKey(), -666) != -666;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        super.program(vertexProgram);
        this.memory.addVertexProgramMemoryComputeKeys(this.vertexProgram);
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        vertexProgram.storeState(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, this.giraphConfiguration);
        this.vertexProgram.getMessageCombiner().ifPresent(combiner -> this.giraphConfiguration.setMessageCombinerClass(GiraphMessageCombiner.class));
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        super.validateStatePriorToExecution();

        return ComputerSubmissionHelper.runWithBackgroundThread(this::submitWithExecutor, "GiraphSubmitter");
    }

    private Future<ComputerResult> submitWithExecutor(Executor exec) {
        final long startTime = System.currentTimeMillis();
        final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(this.giraphConfiguration);
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            try {
                final FileSystem fs = FileSystem.get(this.giraphConfiguration);
                this.loadJars(fs);
                fs.delete(new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)), true);
                ToolRunner.run(this, new String[]{});
            } catch (final Exception e) {
                //e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }

            this.memory.setRuntime(System.currentTimeMillis() - startTime);
            return new DefaultComputerResult(InputOutputHelper.getOutputGraph(apacheConfiguration, this.resultGraph, this.persist), this.memory.asImmutable());
        }, exec);
    }

    @Override
    public int run(final String[] args) {
        this.giraphConfiguration.setBoolean(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, this.persist.equals(Persist.EDGES));
        try {
            // it is possible to run graph computer without a vertex program (and thus, only map reduce jobs if they exist)
            if (null != this.vertexProgram) {
                // a way to verify in Giraph whether the traversal will go over the wire or not
                try {
                    VertexProgram.createVertexProgram(this.hadoopGraph, ConfUtil.makeApacheConfiguration(this.giraphConfiguration));
                } catch (IllegalStateException e) {
                    if (e.getCause() instanceof NumberFormatException)
                        throw new NotSerializableException("The provided traversal is not serializable and thus, can not be distributed across the cluster");
                }
                // prepare the giraph vertex-centric computing job
                final GiraphJob job = new GiraphJob(this.giraphConfiguration, Constants.GREMLIN_HADOOP_GIRAPH_JOB_PREFIX + this.vertexProgram);
                // split required workers across system (open map slots + max threads per machine = total amount of TinkerPop workers)
                if (!this.useWorkerThreadsInConfiguration) {
                    final Cluster cluster = new Cluster(GiraphGraphComputer.this.giraphConfiguration);
                    int totalMappers = cluster.getClusterStatus().getMapSlotCapacity() - 1; // 1 is needed for master
                    cluster.close();
                    if (this.workers <= totalMappers) {
                        this.giraphConfiguration.setWorkerConfiguration(this.workers, this.workers, 100.0F);
                        this.giraphConfiguration.setNumComputeThreads(1);
                    } else {
                        if (totalMappers == 0) totalMappers = 1; // happens in local mode
                        int threadsPerMapper = Long.valueOf(Math.round((double) this.workers / (double) totalMappers)).intValue(); // TODO: need to find least common denominator
                        this.giraphConfiguration.setWorkerConfiguration(totalMappers, totalMappers, 100.0F);
                        this.giraphConfiguration.setNumComputeThreads(threadsPerMapper);
                    }
                }
                // handle input paths (if any)
                if (FileInputFormat.class.isAssignableFrom(this.giraphConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class))) {
                    final Path inputPath = new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
                    if (!FileSystem.get(this.giraphConfiguration).exists(inputPath))  // TODO: what about when the input is not a file input?
                        throw new IllegalArgumentException("The provided input path does not exist: " + inputPath);
                    FileInputFormat.setInputPaths(job.getInternalJob(), inputPath);
                }
                // handle output paths
                final Path outputPath = new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.HIDDEN_G);
                FileOutputFormat.setOutputPath(job.getInternalJob(), outputPath);
                job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
                this.logger.info(Constants.GREMLIN_HADOOP_GIRAPH_JOB_PREFIX + this.vertexProgram);
                // execute the job and wait until it completes (if it fails, throw an exception)
                if (!job.run(true))
                    throw new IllegalStateException("The GiraphGraphComputer job failed -- aborting all subsequent MapReduce jobs");  // how do I get the exception that occured?
                // add vertex program memory values to the return memory
                for (final String key : this.vertexProgram.getMemoryComputeKeys()) {
                    final Path path = new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + key);
                    final ObjectWritableIterator iterator = new ObjectWritableIterator(this.giraphConfiguration, path);
                    if (iterator.hasNext()) {
                        this.memory.set(key, iterator.next().getValue());
                    }
                    FileSystem.get(this.giraphConfiguration).delete(path, true);
                }
                final Path path = new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.HIDDEN_ITERATION);
                this.memory.setIteration((Integer) new ObjectWritableIterator(this.giraphConfiguration, path).next().getValue());
                FileSystem.get(this.giraphConfiguration).delete(path, true);
            }
            // do map reduce jobs
            this.giraphConfiguration.setBoolean(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT_HAS_EDGES, this.giraphConfiguration.getBoolean(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, true));
            for (final MapReduce mapReduce : this.mapReducers) {
                this.memory.addMapReduceMemoryKey(mapReduce);
                MapReduceHelper.executeMapReduceJob(mapReduce, this.memory, this.giraphConfiguration);
            }

            // if no persistence, delete the map reduce output
            if (this.persist.equals(Persist.NOTHING)) {
                final Path outputPath = new Path(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.HIDDEN_G);
                if (FileSystem.get(this.giraphConfiguration).exists(outputPath))      // TODO: what about when the output is not a file output?
                    FileSystem.get(this.giraphConfiguration).delete(outputPath, true);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public void setConf(final org.apache.hadoop.conf.Configuration configuration) {
        // TODO: is this necessary to implement?
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConf() {
        return this.giraphConfiguration;
    }

    private void loadJars(final FileSystem fs) {
        final String hadoopGremlinLibsRemote = "hadoop-gremlin-" + Gremlin.version() + "-libs";
        if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLibsLocal = null == System.getProperty(Constants.HADOOP_GREMLIN_LIBS) ? System.getenv(Constants.HADOOP_GREMLIN_LIBS) : System.getProperty(Constants.HADOOP_GREMLIN_LIBS);
            if (null == hadoopGremlinLibsLocal)
                this.logger.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final String[] paths = hadoopGremlinLibsLocal.split(":");
                for (final String path : paths) {
                    final File file = new File(path);
                    if (file.exists()) {
                        Stream.of(file.listFiles()).filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
                            try {
                                final Path jarFile = new Path(fs.getHomeDirectory() + "/" + hadoopGremlinLibsRemote + "/" + f.getName());
                                if (!fs.exists(jarFile))
                                    fs.copyFromLocalFile(new Path(f.getPath()), jarFile);
                                try {
                                    DistributedCache.addArchiveToClassPath(jarFile, this.giraphConfiguration, fs);
                                } catch (final Exception e) {
                                    throw new RuntimeException(e.getMessage(), e);
                                }
                            } catch (Exception e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                        });
                    } else {
                        this.logger.warn(path + " does not reference a valid directory -- proceeding regardless");
                    }
                }
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new GiraphGraphComputer(HadoopGraph.open(configuration)).program(VertexProgram.createVertexProgram(HadoopGraph.open(configuration), configuration)).submit().get();
    }

    public Features features() {
        return new Features();
    }

    public class Features extends AbstractHadoopGraphComputer.Features {

        @Override
        public int getMaxWorkers() {
            if (GiraphGraphComputer.this.giraphConfiguration.getLocalTestMode())
                return Runtime.getRuntime().availableProcessors();
            else {
                try {
                    final Cluster cluster = new Cluster(GiraphGraphComputer.this.giraphConfiguration);
                    int maxWorkers = (cluster.getClusterStatus().getMapSlotCapacity() - 1) * 16; // max 16 threads per machine hardcoded :|
                    cluster.close();
                    return maxWorkers;
                } catch (final IOException | InterruptedException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }
    }
}
