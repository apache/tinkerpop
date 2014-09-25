package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ScriptTraversal<S, E> {

    public ScriptTraversal<S,E> script(final String script);

    public ScriptTraversal<S, E> over(final Graph graph);

    public ScriptTraversal<S, E> using(final GraphComputer graphComputer);

    public TraversalVertexProgram program();

    public Future<Traversal<S, E>> traversal();

    public Future<ComputerResult> result();

    public Supplier<Traversal<S,E>> supplier();

    public String name();

    // provide helper methods for validation
}
