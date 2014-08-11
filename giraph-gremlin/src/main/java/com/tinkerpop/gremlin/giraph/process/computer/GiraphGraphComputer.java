package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.MapReduceHelper;
import com.tinkerpop.gremlin.giraph.process.computer.util.SideEffectsMapReduce;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.giraph.structure.io.EmptyOutEdges;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Graph;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer extends Configured implements GraphComputer, Tool {

    public static final Logger LOGGER = LoggerFactory.getLogger(GiraphGraphComputer.class);

    protected final GiraphGraph giraphGraph;
    protected GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    private boolean executed = false;

    private final List<MapReduce> mapReduces = new ArrayList<>();
    private VertexProgram vertexProgram;
    final GiraphGraphShellComputerSideEffects sideEffects = new GiraphGraphShellComputerSideEffects();

    public GiraphGraphComputer(final GiraphGraph giraphGraph) {
        this.giraphGraph = giraphGraph;
        final Configuration configuration = giraphGraph.variables().getConfiguration();
        configuration.getKeys().forEachRemaining(key -> this.giraphConfiguration.set(key, configuration.getProperty(key).toString()));
        this.giraphConfiguration.setMasterComputeClass(GiraphGraphComputerSideEffects.class);
        this.giraphConfiguration.setVertexClass(GiraphInternalVertex.class);
        this.giraphConfiguration.setOutEdgesClass(EmptyOutEdges.class);
        this.giraphConfiguration.setClass("giraph.vertexIdClass", LongWritable.class, LongWritable.class);
        this.giraphConfiguration.setClass("giraph.vertexValueClass", Text.class, Text.class);
    }

    public GraphComputer isolation(final Isolation isolation) {
        if (!isolation.equals(Isolation.BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        final Configuration apacheConfiguration = new BaseConfiguration();
        vertexProgram.storeState(apacheConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(apacheConfiguration, this.giraphConfiguration);
        return this;
    }

    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

    public static void mergeComputedView(final Graph original, final Graph computed, Map<String, String> keyMapping) {
        throw new UnsupportedOperationException("GiraphGraphComputer does not support merge computed view as this does not make sense in a Hadoop environment where the graph is fully copied");
    }

    public String toString() {
        return StringFactory.computerString(this);
    }

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
                final String bspDirectory = "bsp_" + UUID.randomUUID().toString();
                final FileSystem fs = FileSystem.get(this.giraphConfiguration);
                fs.delete(new Path(this.giraphConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION)), true);
                final String giraphGremlinHome = System.getenv(Constants.GIRAPH_GREMLIN_HOME);
                if (null == giraphGremlinHome)
                    throw new RuntimeException("Please set $GIRAPH_GREMLIN_HOME to the location of giraph-gremlin");
                final File file = new File(giraphGremlinHome + "/lib");
                if (file.exists()) {
                    if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_JARS_IN_DISTRIBUTED_CACHE, true)) {
                        Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
                            try {
                                fs.copyFromLocalFile(new Path(f.getPath()), new Path(fs.getHomeDirectory() + "/" + bspDirectory + "/" + f.getName()));
                                LOGGER.debug("Loading: " + f.getPath());
                                try {
                                    DistributedCache.addArchiveToClassPath(new Path(fs.getHomeDirectory() + "/" + bspDirectory + "/" + f.getName()), this.giraphConfiguration, fs);
                                } catch (final Exception e) {
                                    throw new RuntimeException(e.getMessage(), e);
                                }
                            } catch (Exception e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                        });
                    }
                } else {
                    LOGGER.warn("No jars loaded from $GIRAPH_GREMLIN_HOME as there is no /lib directory. Attempting to proceed regardless.");
                }
                ToolRunner.run(this, new String[]{});
                // sideEffects.keys().forEach(k -> LOGGER.error(k + "---" + sideEffects.get(k)));
                fs.delete(new Path(bspDirectory), true);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.sideEffects.complete(System.currentTimeMillis() - startTime);
            return new ComputerResult(this.giraphGraph.getOutputGraph(), this.sideEffects);
        });
    }

    public int run(final String[] args) {
        try {
            // it is possible to run graph computer without a vertex program (and thus, only map reduce jobs if they exist)
            if (null != this.vertexProgram) {
                final GiraphJob job = new GiraphJob(this.giraphConfiguration, Constants.GIRAPH_GREMLIN_JOB_PREFIX + this.vertexProgram);
                FileInputFormat.setInputPaths(job.getInternalJob(), new Path(this.giraphConfiguration.get(Constants.GREMLIN_INPUT_LOCATION)));
                FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(this.giraphConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION) + "/" + Constants.TILDA_G));
                // job.getInternalJob().setJarByClass(GiraphGraphComputer.class);
                LOGGER.info(Constants.GIRAPH_GREMLIN_JOB_PREFIX + this.vertexProgram);
                job.run(true);
                this.mapReduces.addAll(this.vertexProgram.getMapReducers());
                // calculate main vertex program sideEffects if desired (costs one mapreduce job)
                if (this.giraphConfiguration.getBoolean(Constants.GREMLIN_DERIVE_COMPUTER_SIDE_EFFECTS, false)) {
                    final Set<String> sideEffectKeys = new HashSet<String>(this.vertexProgram.getSideEffectComputeKeys());
                    sideEffectKeys.add(Constants.ITERATION);
                    this.giraphConfiguration.setStrings(Constants.GREMLIN_SIDE_EFFECT_KEYS, (String[]) sideEffectKeys.toArray(new String[sideEffectKeys.size()]));
                    this.mapReduces.add(new SideEffectsMapReduce(sideEffectKeys));
                }
            }
            // do map reduce jobs
            for (final MapReduce mapReduce : this.mapReduces) {
                MapReduceHelper.executeMapReduceJob(mapReduce, this.sideEffects, this.giraphConfiguration);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage(), e);
        }
        return 0;
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
