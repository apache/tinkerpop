package com.tinkerpop.blueprints.global;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphComputer {

    public enum Isolation {
        /**
         * Computations are carried out in a bulk synchronous manner.
         * The results of a vertex property update are only visible after the round is complete.
         */
        BSP,
        /**
         * Computations are carried out in a bulk synchronous manner.
         * The results of a vertex property update are visible before the end of the round.
         */
        DIRTY_BSP
    }

    /**
     * Execute the GraphComputer's VertexProgram against the GraphComputer's graph.
     * The GraphComputer must have reference to a VertexProgram and Graph.
     * The typical flow of execution is:
     *  1. Set up the VertexMemory as necessary given the VertexProgram (e.g. set compute keys).
     *  2. Set up the GraphMemory as necessary given the VertexProgram.
     *  3. Execute the VertexProgram
     */
    public void execute();

    /**
     * Get the VertexSystemMemory cast as a VertexMemory to hide system specific methods.
     *
     * @return the GraphComputer's VertexMemory
     */
    public VertexMemory getVertexMemory();

    /**
     * Get the GraphSystemMemory cast as a GraphMemory to hide system specific methods.
     *
     * @return the GraphComputer's GraphMemory
     */
    public GraphMemory getGraphMemory();
}
