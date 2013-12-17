package com.tinkerpop.blueprints;

/**
 * Direction is used to denote the direction of an edge or location of a vertex on an edge.
 * For example, gremlin--knows-->rexster is an OUT edge for Gremlin and an IN edge for Rexster.
 * Moreover, given that edge, Gremlin is the OUT vertex and Rexster is the IN vertex.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Direction {

    OUT, IN, BOTH;

    public static final Direction[] proper = new Direction[]{OUT, IN};

    public Direction opposite() {
        if (this.equals(OUT))
            return IN;
        else if (this.equals(IN))
            return OUT;
        else
            return BOTH;
    }
}
