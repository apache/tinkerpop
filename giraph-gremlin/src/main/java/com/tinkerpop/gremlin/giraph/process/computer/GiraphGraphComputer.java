package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.giraph.process.computer.util.MemoryMapReduce;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.GiraphHelper;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphGraphComputer extends Configured implements GraphComputer, Tool {

    public static final Logger LOGGER = LoggerFactory.getLogger(GiraphGraphComputer.class);

    protected final GiraphGraph giraphGraph;
    protected GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    private boolean executed = false;

    private final Set<MapReduce> mapReduces = new HashSet<>();
    private VertexProgram vertexProgram;
    final GiraphImmutableMemory memory = new GiraphImmutableMemory();

    public GiraphGraphComputer(final GiraphGraph giraphGraph) {
        this.giraphGraph = giraphGraph;
        final Configuration configuration = giraphGraph.configuration();
        configuration.getKeys().forEachRemaining(key -> this.giraphConfiguration.set(key, configuration.getProperty(key).toString()));
        this.giraphConfiguration.setMasterComputeClass(GiraphMemory.class);
        this.giraphConfiguration.setVertexClass(GiraphComputeVertex.class);
        this.giraphConfiguration.setWorkerContextClass(GiraphWorkerContext.class);
        this.giraphConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        this.giraphConfiguration.setClass("giraph.vertexIdClass", LongWritable.class, LongWritable.class);
        this.giraphConfiguration.setClass("giraph.vertexValueClass", Text.class, Text.class);
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
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        vertexProgram.storeState(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, this.giraphConfiguration);
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

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
        if (null == this.vertexProgram && this.mapReduces.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram)
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        final long startTime = System.currentTimeMillis();
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            try {
                final FileSystem fs = FileSystem.get(this.giraphConfiguration);
                this.loadJars(fs);
                fs.delete(new Path(this.giraphConfiguration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION)), true);
                ToolRunner.run(this, new String[]{});
                // memory.keys().forEach(k -> LOGGER.error(k + "---" + memory.get(k)));
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.memory.complete(System.currentTimeMillis() - startTime);
            return new ComputerResult(GiraphHelper.getOutputGraph(this.giraphGraph), this.memory);
        });
    }

    @Override
    public int run(final String[] args) {
        try {
            // it is possible to run graph computer without a vertex program (and thus, only map reduce jobs if they exist)
            if (null != this.vertexProgram) {
                final GiraphJob job = new GiraphJob(this.giraphConfiguration, Constants.GIRAPH_GREMLIN_JOB_PREFIX + this.vertexProgram);
                final Path inputPath = new Path(this.giraphConfiguration.get(Constants.GREMLIN_GIRAPH_INPUT_LOCATION));
                if (!FileSystem.get(this.giraphConfiguration).exists(inputPath))
                    throw new IllegalArgumentException("The provided input path does not exist: " + inputPath);
                FileInputFormat.setInputPaths(job.getInternalJob(), inputPath);
                FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(this.giraphConfiguration.get(Constants.GREMLIN_GIRAPH_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G));
                // job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
                LOGGER.info(Constants.GIRAPH_GREMLIN_JOB_PREFIX + this.vertexProgram);
                if (!job.run(true)) {
                    throw new IllegalStateException("The Giraph-Gremlin job failed -- aborting all subsequent MapReduce jobs");
                }
                this.mapReduces.addAll(this.vertexProgram.getMapReducers());
                // calculate main vertex program memory if desired (costs one mapreduce job)
                if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_GIRAPH_DERIVE_MEMORY, false)) {
                    final Set<String> memoryKeys = new HashSet<String>(this.vertexProgram.getMemoryComputeKeys());
                    memoryKeys.add(Constants.SYSTEM_ITERATION);
                    this.giraphConfiguration.setStrings(Constants.GREMLIN_GIRAPH_MEMORY_KEYS, (String[]) memoryKeys.toArray(new String[memoryKeys.size()]));
                    this.mapReduces.add(new MemoryMapReduce(memoryKeys));
                }
            }
            // do map reduce jobs
            for (final MapReduce mapReduce : this.mapReduces) {
                MapReduceHelper.executeMapReduceJob(mapReduce, this.memory, this.giraphConfiguration);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
            throw new IllegalStateException(e.getMessage(), e);
        }
        return 0;
    }

    private void loadJars(final FileSystem fs) {
        final String giraphGremlinLibsRemote = "giraph-gremlin-libs";
        if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_GIRAPH_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String giraphGremlinLibsLocal = System.getenv(Constants.GIRAPH_GREMLIN_LIBS);
            if (null == giraphGremlinLibsLocal)
                LOGGER.warn(Constants.GIRAPH_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final File file = new File(giraphGremlinLibsLocal);
                if (file.exists()) {
                    Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
                        try {
                            final Path jarFile = new Path(fs.getHomeDirectory() + "/" + giraphGremlinLibsRemote + "/" + f.getName());
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
                    LOGGER.warn(Constants.GIRAPH_GREMLIN_LIBS + " does not reference a valid directory -- proceeding regardless: " + giraphGremlinLibsLocal);
                }
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        try {
            final FileConfiguration configuration = new PropertiesConfiguration();
            configuration.load(new File(args[0]));
            final GiraphGraphComputer computer = new GiraphGraphComputer(GiraphGraph.open(configuration));
            computer.program(VertexProgram.createVertexProgram(configuration)).submit().get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
