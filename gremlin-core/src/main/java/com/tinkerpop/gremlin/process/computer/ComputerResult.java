package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * The result of the {@link GraphComputer}'s computation. This is returned in a {@link java.util.concurrent.Future} by GraphComputer.submit().
 * A GraphComputer computation yields two things: an updated view of the computed on {@link Graph} and any computational sideEffects called {@link Memory}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerResult {

    private final Graph graph;
    private final Memory memory;

    public ComputerResult(final Graph graph, final Memory memory) {
        this.graph = graph;
        this.memory = memory;
    }

    /**
     * Get the view of the original {@link Graph} computed on by the GraphComputer.
     *
     * @return The computed graph
     */
    public Graph graph() {
        return this.graph;
    }

    /**
     * Get the computational sideEffects called {@link Memory} of the GraphComputer.
     *
     * @return the computed memory
     */
    public Memory memory() {
        return this.memory;
    }

    public String toString() {
        return "result[" + this.graph + "," + this.memory + "]";
    }
}
