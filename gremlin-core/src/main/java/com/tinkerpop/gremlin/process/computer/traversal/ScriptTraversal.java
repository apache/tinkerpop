package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ScriptTraversal<S, E> {

    public static <S, E> ScriptTraversal<S, E> of(final String traversalScript) {
        throw new UnsupportedOperationException("Implementation must implement this method: ");
    }

    public ScriptTraversal<S, E> over(final Graph graph);

    public ScriptTraversal<S, E> using(final GraphComputer graphComputer);

    public TraversalVertexProgram program();

    public Future<Traversal<S, E>> traversal();

    public Future<ComputerResult> result();

    public static String transformToGlobalScan(final String traversalScript) {
        return traversalScript;
    }

}
