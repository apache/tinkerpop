package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.function.BiConsumer;

/**
 * An array of key/value maps accessible by index.
 * The total size of the enumerator may be unknown when it is created;
 * it grows when successive solutions are requested, computing as many as necessary.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Enumerator<T> {

    /**
     * @return the number of solutions so far known; the enumerator has at least this many.
     * This should be a relatively cheap operation.
     */
    int size();

    /**
     * Provides access to a solution, allowing it to be printed, put into a map, etc.
     * @param index the index of the solution
     * @param visitor a consumer for each key/value pair in the solution
     * @return whether a solution exists at the given index and was visited
     */
    boolean visitSolution(int index, BiConsumer<String, T> visitor);
}
