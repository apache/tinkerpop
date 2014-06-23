package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalResult;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    public static final String G = "g";
    public static final String GLOBALS = "globals";

    public static final String GIRAPH_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    private static final String GIRAPH_GREMLIN_HOME = "GIRAPH_GREMLIN_HOME";
    private static final String DOT_JAR = ".jar";
    private static final Logger LOGGER = LoggerFactory.getLogger(GiraphGraphComputer.class);

    public static final String GREMLIN_INPUT_LOCATION = "gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "gremlin.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    public static final String GREMLIN_EXTRA_JOBS_CALCULATOR = "gremlin.extraJobsCalculator";
    public static final String GREMLIN_DERIVE_GLOBALS = "gremlin.deriveGlobals";

    protected final GiraphGraph giraphGraph;
    protected org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

    public GiraphGraphComputer(final GiraphGraph giraphGraph, final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        this.giraphGraph = giraphGraph;
    }

    public GraphComputer isolation(final Isolation isolation) {
        if (isolation.equals(Isolation.DIRTY_BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    public GraphComputer program(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        return this;
    }

    public static void mergeComputedView(final Graph original, final Graph computed, Map<String, String> keyMapping) {
        throw new UnsupportedOperationException("GiraphGraphComputer does not support merge computed view as this does not make sense in a Hadoop environment");
    }

    public Future<Pair<Graph, Globals>> submit() {
        return CompletableFuture.<Pair<Graph, Globals>>supplyAsync(() -> {
            try {
                final String bspDirectory = "_bsp"; //"temp-" + UUID.randomUUID().toString();
                final FileSystem fs = FileSystem.get(this.hadoopConfiguration);
                fs.delete(new Path(this.hadoopConfiguration.get(GREMLIN_OUTPUT_LOCATION)), true);
                final String giraphGremlinHome = System.getenv(GIRAPH_GREMLIN_HOME);
                if (null == giraphGremlinHome)
                    throw new RuntimeException("Please set $GIRAPH_GREMLIN_HOME to the location of giraph-gremlin");
                final File file = new File(giraphGremlinHome + "/lib");
                if (file.exists()) {
                    Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(DOT_JAR)).forEach(f -> {
                        try {
                            fs.copyFromLocalFile(new Path(f.getPath()), new Path(fs.getHomeDirectory() + "/" + bspDirectory + "/" + f.getName()));
                            LOGGER.debug("Loading: " + f.getPath());
                            try {
                                DistributedCache.addArchiveToClassPath(new Path(fs.getHomeDirectory() + "/" + bspDirectory + "/" + f.getName()), this.hadoopConfiguration, fs);
                            } catch (final Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }
                        } catch (Exception e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    });
                } else {
                    LOGGER.warn("No jars loaded from $GIRAPH_GREMLIN_HOME as there is no /lib directory. Attempting to proceed regardless.");
                }
                ToolRunner.run(new GiraphGraphRunner(this.hadoopConfiguration), new String[]{});
                fs.delete(new Path(bspDirectory), true);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            // LOGGER.info(new GiraphGraphShellComputerGlobals(this.hadoopConfiguration).asMap().toString());
            // TODO: Should point to the manipulated graph
            return new Pair<Graph, Globals>(this.giraphGraph, new GiraphGraphShellComputerGlobals(this.hadoopConfiguration));
        });
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        ((TraverserSource) traversal.getSteps().get(0)).clear();
        return new TraversalResult<>(this.giraphGraph, () -> traversal);
    }
}
