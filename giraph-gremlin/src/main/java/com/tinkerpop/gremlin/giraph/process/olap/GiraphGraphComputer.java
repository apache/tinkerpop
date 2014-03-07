package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    private GiraphConfiguration giraphConfiguration = new GiraphConfiguration(new org.apache.hadoop.conf.Configuration(true));
    private VertexProgram vertexProgram;

    public GraphComputer isolation(final Isolation isolation) {
        if (isolation.equals(Isolation.DIRTY_BSP))
            throw GraphComputer.Exceptions.isolationNotSupported(isolation);
        return this;
    }

    public GraphComputer program(final VertexProgram program) {
        this.vertexProgram = program;
        return this;
    }

    public GraphComputer configuration(final Configuration configuration) {
        this.giraphConfiguration = new GiraphConfiguration();
        configuration.getKeys().forEachRemaining(key -> this.giraphConfiguration.set(key, configuration.getString(key)));
        return this;
    }

    public Future<Graph> submit() {
        try {
            ToolRunner.run(new GiraphGraphRunner(this.giraphConfiguration), new String[]{});
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return null;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return Collections.emptyIterator();
    }
}
