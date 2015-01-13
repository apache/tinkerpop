package com.tinkerpop.gremlin.process.traverser;

/**
 * A {@link TraverserRequirement} is a list of requirements that a {@link com.tinkerpop.gremlin.process.Traversal} requires of a {@link com.tinkerpop.gremlin.process.Traverser}.
 * The less requirements, the simpler the traverser can be (both in terms of space and time constraints).
 * Every {@link com.tinkerpop.gremlin.process.Step} provides its specific requirements via {@link com.tinkerpop.gremlin.process.Step#getRequirements()}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum TraverserRequirement {

    OBJECT,
    BULK,
    SINGLE_LOOP,
    NESTED_LOOP,
    PATH,
    PATH_ACCESS,
    SACK,
    SIDE_EFFECTS
}
