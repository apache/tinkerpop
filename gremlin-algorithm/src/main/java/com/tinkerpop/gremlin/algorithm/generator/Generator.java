package com.tinkerpop.gremlin.algorithm.generator;

/**
 * Interface for Graph generators.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Generator {
    /**
     * Generate the elements in the graph given the nature of the implementation and return the number of
     * edges generated as a result.
     *
     * @return number of edges generated
     */
    public int generate();
}
