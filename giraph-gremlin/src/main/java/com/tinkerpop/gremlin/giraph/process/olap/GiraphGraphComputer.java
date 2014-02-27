package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputer implements GraphComputer {

    public GraphComputer isolation(final Isolation isolation) {
        return this;
    }

    public GraphComputer program(final VertexProgram program) {
        return this;
    }

    public GraphComputer configuration(final Configuration configuration) {
        return this;
    }

    public Future<Graph> submit() {
        return null;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return Collections.emptyIterator();
    }
}
