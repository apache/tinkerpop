package com.tinkerpop.gremlin.process.traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum TraverserRequirements {

    OBJECT,
    BULK,
    SINGLE_LOOP,
    NESTED_LOOP,
    PATH,
    PATH_ACCESS,
    SACK,
    SIDE_EFFECTS
}
