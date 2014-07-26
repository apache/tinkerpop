package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.Set;
import java.util.function.BiConsumer;

/**
 * An array of key/value maps accessible by index.
 * The total size of the enumerator may be unknown when it is created; it grows when successive solutions are requested, computing as many as necessary.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Enumerator<T> {

    /**
     * @return the set of variable names bound to values in each solution
     */
    Set<String> getVariables();

    /**
     * @return the number of solutions so far computed
     */
    int size();

    /**
     * @return whether all solutions have already been computed
     */
    boolean isComplete();

    /**
     * Provides access to a solution, allowing it to be printed, put into a map, etc.
     * @param i the index of the solution
     * @param visitor a consumer for each key/value pair in the solution
     * @return whether a solution exists at the given index and was visited
     */
    boolean visitSolution(int i, BiConsumer<String, T> visitor);
}
