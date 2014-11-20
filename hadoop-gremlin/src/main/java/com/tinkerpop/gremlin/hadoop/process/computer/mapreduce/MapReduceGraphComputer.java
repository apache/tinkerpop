package com.tinkerpop.gremlin.hadoop.process.computer.mapreduce;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapReduceGraphComputer extends Configured implements GraphComputer, Tool {

    public static final Logger LOGGER = LoggerFactory.getLogger(MapReduceGraphComputer.class);

    protected final HadoopGraph hadoopGraph;
    protected org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
    private boolean executed = false;

    private final Set<MapReduce> mapReduces = new HashSet<>();
    private VertexProgram vertexProgram;
    final HadoopImmutableMemory memory = new HadoopImmutableMemory();

    public MapReduceGraphComputer(final HadoopGraph hadoopGraph) {
        this.hadoopGraph = hadoopGraph;
        final Configuration configuration = hadoopGraph.configuration();
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        //this.hadoopConfiguration.setMasterComputeClass(GiraphMemory.class);
        //this.hadoopConfiguration.setVertexClass(GiraphComputeVertex.class);
        //this.hadoopConfiguration.setWorkerContextClass(GiraphWorkerContext.class);
        //this.hadoopConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        //this.hadoopConfiguration.setClass("giraph.vertexIdClass", LongWritable.class, LongWritable.class);
        //this.hadoopConfiguration.setClass("giraph.vertexValueClass", Text.class, Text.class);
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
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, this.hadoopConfiguration);
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
                final FileSystem fs = FileSystem.get(this.hadoopConfiguration);
                this.loadJars(fs);
                fs.delete(new Path(this.hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION)), true);
                ToolRunner.run(this, new String[]{});
                // memory.keys().forEach(k -> LOGGER.error(k + "---" + memory.get(k)));
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.memory.complete(System.currentTimeMillis() - startTime);
            return new ComputerResult(ConfUtil.getOutputGraph(this.hadoopGraph), this.memory);
        });
    }

    @Override
    public int run(final String[] args) {
        try {
            // it is possible to run graph computer without a vertex program (and thus, only map reduce jobs if they exist)
            if (null != this.vertexProgram) {
                final Job job = new Job(this.hadoopConfiguration, Constants.GIRAPH_HADOOP_JOB_PREFIX + this.vertexProgram);
                final Path inputPath = new Path(this.hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION));
                if (!FileSystem.get(this.hadoopConfiguration).exists(inputPath))
                    throw new IllegalArgumentException("The provided input path does not exist: " + inputPath);
                FileInputFormat.setInputPaths(job, inputPath);
                FileOutputFormat.setOutputPath(job, new Path(this.hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + Constants.SYSTEM_G));
                job.setJarByClass(HadoopGraph.class);
                // TODO: need a BSPMapReduce and a "isDone?" loop
                LOGGER.info(Constants.GIRAPH_HADOOP_JOB_PREFIX + this.vertexProgram);
                if (!job.waitForCompletion(true)) {
                    throw new IllegalStateException("The Hadoop-Gremlin job failed -- aborting all subsequent MapReduce jobs");
                }
                this.mapReduces.addAll(this.vertexProgram.getMapReducers());
                // calculate main vertex program memory if desired (costs one mapreduce job)
                if (this.hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_DERIVE_MEMORY, false)) {
                    final Set<String> memoryKeys = new HashSet<String>(this.vertexProgram.getMemoryComputeKeys());
                    memoryKeys.add(Constants.SYSTEM_ITERATION);
                    this.hadoopConfiguration.setStrings(Constants.GREMLIN_HADOOP_MEMORY_KEYS, (String[]) memoryKeys.toArray(new String[memoryKeys.size()]));
                    // TODO: this.mapReduces.add(new MemoryMapReduce(memoryKeys));
                }
            }
            // do map reduce jobs
            for (final MapReduce mapReduce : this.mapReduces) {
                MapReduceHelper.executeMapReduceJob(mapReduce, this.memory, this.hadoopConfiguration);
            }
        } catch (final Exception e) {
            // e.printStackTrace();
            throw new IllegalStateException(e.getMessage(), e);
        }
        return 0;
    }

    private void loadJars(final FileSystem fs) {
        final String giraphGremlinLibsRemote = "giraph-gremlin-libs";
        if (this.hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String giraphGremlinLibsLocal = System.getenv(Constants.HADOOP_GREMLIN_LIBS);
            if (null == giraphGremlinLibsLocal)
                LOGGER.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final String[] paths = giraphGremlinLibsLocal.split(":");
                for (final String path : paths) {
                    final File file = new File(path);
                    if (file.exists()) {
                        Stream.of(file.listFiles()).filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
                            try {
                                final Path jarFile = new Path(fs.getHomeDirectory() + "/" + giraphGremlinLibsRemote + "/" + f.getName());
                                fs.copyFromLocalFile(new Path(f.getPath()), jarFile);
                                try {
                                    DistributedCache.addArchiveToClassPath(jarFile, this.hadoopConfiguration, fs);
                                } catch (final Exception e) {
                                    throw new RuntimeException(e.getMessage(), e);
                                }
                            } catch (Exception e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                        });
                    } else {
                        LOGGER.warn(path + " does not reference a valid directory -- proceeding regardless");
                    }
                }
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        try {
            final FileConfiguration configuration = new PropertiesConfiguration();
            configuration.load(new File(args[0]));
            final MapReduceGraphComputer computer = new MapReduceGraphComputer(HadoopGraph.open(configuration));
            computer.program(VertexProgram.createVertexProgram(configuration)).submit().get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
