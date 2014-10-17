package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalScript<S, E> {

    public TraversalScript<S, E> over(final Graph graph);

    public TraversalScript<S, E> using(final GraphComputer graphComputer);

    public TraversalVertexProgram program();

    public Future<Traversal<S, E>> traversal();

    public Future<ComputerResult> result();
}
