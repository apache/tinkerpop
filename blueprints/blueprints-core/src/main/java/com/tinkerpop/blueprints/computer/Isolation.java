package com.tinkerpop.blueprints.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
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