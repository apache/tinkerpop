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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
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
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritableIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphGraphComputer extends AbstractHadoopGraphComputer implements GraphComputer, Tool {

    protected GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    private MapMemory memory = new MapMemory();
    private boolean useWorkerThreadsInConfiguration;
    private Set<String> vertexProgramConfigurationKeys = new HashSet<>();

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
        apacheConfiguration.setDelimiterParsingDisabled(true);
        vertexProgram.storeState(apacheConfiguration);
        IteratorUtils.fill(apacheConfiguration.getKeys(), this.vertexProgramConfigurationKeys);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, this.giraphConfiguration);
        this.vertexProgram.getMessageCombiner().ifPresent(combiner -> this.giraphConfiguration.setMessageCombinerClass(GiraphMessageCombiner.class));
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        super.validateStatePriorToExecution();
        return ComputerSubmissionHelper.runWithBackgroundThread(this::submitWithExecutor, "GiraphSubmitter");
    }

    private Future<ComputerResult> submitWithExecutor(final Executor exec) {
        final long startTime = System.currentTimeMillis();
        final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(this.giraphConfiguration);
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            try {
                this.loadJars(giraphConfiguration);
                ToolRunner.run(this, new String[]{});
            } catch (final Exception e) {
                //e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.memory.setRuntime(System.currentTimeMillis() - startTime);
            // clear properties that should not be propagated in an OLAP chain
            apacheConfiguration.clearProperty(Constants.GREMLIN_HADOOP_GRAPH_FILTER);
            apacheConfiguration.clearProperty(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR);
            this.vertexProgramConfigurationKeys.forEach(apacheConfiguration::clearProperty); // clear out vertex program specific configurations
            return new DefaultComputerResult(InputOutputHelper.getOutputGraph(apacheConfiguration, this.resultGraph, this.persist), this.memory.asImmutable());
        }, exec);
    }

    @Override
    public int run(final String[] args) {
        final Storage storage = FileSystemStorage.open(this.giraphConfiguration);
        storage.rm(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
        this.giraphConfiguration.setBoolean(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, this.persist.equals(Persist.EDGES));
        try {
            // store vertex and edge filters (will propagate down to native InputFormat or else GiraphVertexInputFormat will process)
            final BaseConfiguration apacheConfiguration = new BaseConfiguration();
            apacheConfiguration.setDelimiterParsingDisabled(true);
            GraphFilterAware.storeGraphFilter(apacheConfiguration, this.giraphConfiguration, this.graphFilter);

            // it is possible to run graph computer without a vertex program (and thus, only map reduce jobs if they exist)
            if (null != this.vertexProgram) {
                // a way to verify in Giraph whether the traversal will go over the wire or not
                try {
                    VertexProgram.createVertexProgram(this.hadoopGraph, ConfUtil.makeApacheConfiguration(this.giraphConfiguration));
                } catch (final IllegalStateException e) {
                    // NumberFormatException is likely no longer a possibility here after 3.2.9 as the internal
                    // serialization format for traversals changed from a delimited list of bytes as a string to a
                    // base64 encoded string. under the base64 model we shouldn't see NumberFormatException anymore
                    // but i left it here for now, just in case there's something i'm not seeing. see
                    // VertexProgramHelper.deserialize() for more information related to this handling
                    final Throwable root = ExceptionUtils.getRootCause(e);
                    if (root instanceof NumberFormatException || root instanceof IOException || root instanceof ClassNotFoundException)
                        throw new NotSerializableException("The provided traversal is not serializable and thus, can not be distributed across the cluster");
                }
                // remove historic combiners in configuration propagation (this occurs when job chaining)
                if (!this.vertexProgram.getMessageCombiner().isPresent())
                    this.giraphConfiguration.unset(GiraphConstants.MESSAGE_COMBINER_CLASS.getKey());
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
                // prepare the giraph vertex-centric computing job
                final GiraphJob job = new GiraphJob(this.giraphConfiguration, Constants.GREMLIN_HADOOP_GIRAPH_JOB_PREFIX + this.vertexProgram);
                job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
                this.logger.info(Constants.GREMLIN_HADOOP_GIRAPH_JOB_PREFIX + this.vertexProgram);
                // handle input paths (if any)
                String inputLocation = this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION, null);
                if (null != inputLocation && FileInputFormat.class.isAssignableFrom(this.giraphConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, InputFormat.class))) {
                    inputLocation = Constants.getSearchGraphLocation(inputLocation, storage).orElse(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
                    FileInputFormat.setInputPaths(job.getInternalJob(), new Path(inputLocation));
                }
                // handle output paths (if any)
                String outputLocation = this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
                if (null != outputLocation && FileOutputFormat.class.isAssignableFrom(this.giraphConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_WRITER, OutputFormat.class))) {
                    outputLocation = Constants.getGraphLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
                    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outputLocation));
                }
                // execute the job and wait until it completes (if it fails, throw an exception)
                if (!job.run(true))
                    throw new IllegalStateException("The GiraphGraphComputer job failed -- aborting all subsequent MapReduce jobs: " + job.getInternalJob().getStatus().getFailureInfo());
                // add vertex program memory values to the return memory
                for (final MemoryComputeKey memoryComputeKey : this.vertexProgram.getMemoryComputeKeys()) {
                    if (!memoryComputeKey.isTransient() && storage.exists(Constants.getMemoryLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), memoryComputeKey.getKey()))) {
                        final ObjectWritableIterator iterator = new ObjectWritableIterator(this.giraphConfiguration, new Path(Constants.getMemoryLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), memoryComputeKey.getKey())));
                        if (iterator.hasNext()) {
                            this.memory.set(memoryComputeKey.getKey(), iterator.next().getValue());
                        }
                        // vertex program memory items are not stored on disk
                        storage.rm(Constants.getMemoryLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), memoryComputeKey.getKey()));
                    }
                }
                final Path path = new Path(Constants.getMemoryLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), Constants.HIDDEN_ITERATION));
                this.memory.setIteration((Integer) new ObjectWritableIterator(this.giraphConfiguration, path).next().getValue());
                storage.rm(Constants.getMemoryLocation(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), Constants.HIDDEN_ITERATION));
            }
            // do map reduce jobs
            this.giraphConfiguration.setBoolean(Constants.GREMLIN_HADOOP_GRAPH_READER_HAS_EDGES, this.giraphConfiguration.getBoolean(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES, true));
            for (final MapReduce mapReduce : this.mapReducers) {
                this.memory.addMapReduceMemoryKey(mapReduce);
                MapReduceHelper.executeMapReduceJob(mapReduce, this.memory, this.giraphConfiguration);
            }

            // if no persistence, delete the graph and memory output
            if (this.persist.equals(Persist.NOTHING))
                storage.rm(this.giraphConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
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

    @Override
    protected void loadJar(final org.apache.hadoop.conf.Configuration hadoopConfiguration, final File file, final Object... params)
            throws IOException {
        final FileSystem defaultFileSystem = FileSystem.get(hadoopConfiguration);
        try {
            final Path jarFile = new Path(defaultFileSystem.getHomeDirectory() + "/hadoop-gremlin-" + Gremlin.version() + "-libs/" + file.getName());
            if (!defaultFileSystem.exists(jarFile)) {
                final Path sourcePath = new Path(file.getPath());
                final URI sourceUri = sourcePath.toUri();
                final FileSystem fs = FileSystem.get(sourceUri, hadoopConfiguration);
                fs.copyFromLocalFile(sourcePath, jarFile);
            }
            try {
                DistributedCache.addArchiveToClassPath(jarFile, this.giraphConfiguration, defaultFileSystem);
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
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
                return Integer.MAX_VALUE;
                /*try {
                    final Cluster cluster = new Cluster(GiraphGraphComputer.this.giraphConfiguration);
                    int maxWorkers = (cluster.getClusterStatus().getMapSlotCapacity() - 1) * 16; // max 16 threads per machine hardcoded :|
                    cluster.close();
                    return maxWorkers;

                } catch (final IOException | InterruptedException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }*/
            }
        }
    }
}
