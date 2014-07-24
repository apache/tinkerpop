package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.SideEffects;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    public static final Logger LOGGER = LoggerFactory.getLogger(GiraphGraphComputer.class);

    protected final GiraphGraph giraphGraph;
    protected org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

    public GiraphGraphComputer(final GiraphGraph giraphGraph, final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        this.giraphGraph = giraphGraph;
    }

    public GraphComputer isolation(final Isolation isolation) {
        if (!isolation.equals(Isolation.BSP))
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

    public Graph getGraph() {
        return this.giraphGraph;
    }

    public Future<Pair<Graph, SideEffects>> submit() {
        return CompletableFuture.<Pair<Graph, SideEffects>>supplyAsync(() -> {
            final GiraphGraphShellComputerSideEffects sideEffects = new GiraphGraphShellComputerSideEffects(this.hadoopConfiguration);
            try {
                final String bspDirectory = "_bsp"; // "temp-" + UUID.randomUUID().toString();
                final FileSystem fs = FileSystem.get(this.hadoopConfiguration);
                fs.delete(new Path(this.hadoopConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION)), true);
                final String giraphGremlinHome = System.getenv(Constants.GIRAPH_GREMLIN_HOME);
                if (null == giraphGremlinHome)
                    throw new RuntimeException("Please set $GIRAPH_GREMLIN_HOME to the location of giraph-gremlin");
                final File file = new File(giraphGremlinHome + "/lib");
                if (file.exists()) {
                    if (this.hadoopConfiguration.getBoolean(Constants.GREMLIN_JARS_IN_DISTRIBUTED_CACHE, true)) {
                        Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
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
                    }
                } else {
                    LOGGER.warn("No jars loaded from $GIRAPH_GREMLIN_HOME as there is no /lib directory. Attempting to proceed regardless.");
                }
                ToolRunner.run(new GiraphGraphRunner(this.hadoopConfiguration, sideEffects), new String[]{});
                // sideEffects.keys().forEach(k -> LOGGER.error(k + "---" + sideEffects.get(k)));
                fs.delete(new Path(bspDirectory), true);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            // LOGGER.info(new GiraphGraphShellComputerGlobals(this.hadoopConfiguration).asMap().toString());
            return new Pair<>(this.giraphGraph.getOutputGraph(), sideEffects);
        });
    }
}
