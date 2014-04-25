package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    private static final String GIRAPH_GREMLIN_HOME = "GIRAPH_GREMLIN_HOME";
    private static final String DOT_JAR = ".jar";
    private static final Logger LOGGER = Logger.getLogger(GiraphGraphComputer.class);

    public static final String GREMLIN_INPUT_LOCATION = "gremlin.input.location";

    private org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

    public GraphComputer isolation(final Isolation isolation) {
        if (isolation.equals(Isolation.DIRTY_BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    public GraphComputer program(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        return this;
    }

    public Future<Graph> submit() {
        try {
            final FileSystem fs = FileSystem.get(this.hadoopConfiguration);
            fs.delete(new Path(this.hadoopConfiguration.get("mapred.output.dir")), true);
            final FileSystem local = FileSystem.getLocal(this.hadoopConfiguration);
            final String giraphGremlinHome = System.getenv(GIRAPH_GREMLIN_HOME);
            if (null == giraphGremlinHome)
                throw new RuntimeException("Please set $GIRAPH_GREMLIN_HOME to the location of giraph-gremlin");
            final File file = new File(giraphGremlinHome + "/lib");
            if (file.exists()) {
                Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(DOT_JAR)).forEach(f -> {
                    LOGGER.debug("Loading: " + f.getPath());
                    try {
                        DistributedCache.addArchiveToClassPath(new Path(f.getPath()), this.hadoopConfiguration, local);
                    } catch (final Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });
            } else {
                LOGGER.warn("No jars loaded from $GIRAPH_GREMLIN_HOME as there is no /lib directory. Attempting to proceed regardless.");
            }
            ToolRunner.run(new GiraphGraphRunner(this.hadoopConfiguration), new String[]{});
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage(), e);
        }
        return null;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return Collections.emptyIterator();
    }
}
