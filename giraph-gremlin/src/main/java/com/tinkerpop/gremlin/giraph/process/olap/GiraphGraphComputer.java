package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.process.olap.util.ConfUtil;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    private org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

    public GraphComputer isolation(final Isolation isolation) {
        if (isolation.equals(Isolation.DIRTY_BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    public GraphComputer program(final VertexProgram.Builder vertexProgramBuilder) {
        final Configuration configuration = vertexProgramBuilder.build();
        configuration.getKeys().forEachRemaining(key -> this.hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
        return this;
    }

    public GraphComputer configuration(final Configuration configuration) {
        ConfUtil.hadoopConfiguration(configuration).forEach(entry -> this.hadoopConfiguration.set(entry.getKey(), entry.getValue()));
        return this;
    }

    public Future<Graph> submit() {
        try {
            final FileSystem fs = FileSystem.get(this.hadoopConfiguration);
            fs.delete(new Path("output"), true);
            final FileSystem local = FileSystem.getLocal(this.hadoopConfiguration);
            Arrays.asList(
                    "/usr/local/giraph-1.0.0/giraph-core/target/giraph-1.0.0-for-hadoop-1.2.1-jar-with-dependencies.jar",
                    "/Users/marko/software/tinkerpop/tinkerpop3/giraph-gremlin/target/giraph-gremlin-3.0.0-SNAPSHOT-job.jar",
                    "/Users/marko/software/tinkerpop/tinkerpop3/gremlin-core/target/gremlin-core-3.0.0-SNAPSHOT.jar",
                    "/Users/marko/software/tinkerpop/tinkerpop3/tinkergraph-gremlin/target/tinkergraph-gremlin-3.0.0-SNAPSHOT.jar")
                    .forEach(s -> {
                        try {
                            DistributedCache.addArchiveToClassPath(new Path(s), this.hadoopConfiguration, local);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    });
            ToolRunner.run(new GiraphGraphRunner(this.hadoopConfiguration), new String[]{});
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return Collections.emptyIterator();
    }
}
