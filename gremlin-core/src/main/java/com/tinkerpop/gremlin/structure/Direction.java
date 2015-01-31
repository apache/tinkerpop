package com.tinkerpop.gremlin.structure;

/**
 * {@link Direction} is used to denote the direction of an {@link Edge} or location of a {@link Vertex} on an
 * {@link Edge}. For example:
 * <p/>
 * <pre>
 * gremlin--knows-->rexster
 * </pre>
 * is an {@link Direction#OUT} {@link Edge} for Gremlin and an {@link Direction#IN} edge for Rexster. Moreover, given
 * that {@link Edge}, Gremlin is the {@link Direction#OUT} {@link Vertex} and Rexster is the {@link Direction#IN}
 * {@link Vertex}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Direction {

    OUT, IN, BOTH;

    public static final Direction[] proper = new Direction[]{OUT, IN};

    /**
     * Produce the opposite representation of the current {@code Direction} enum.
     */
    public Direction opposite() {
        if (this.equals(OUT))
            return IN;
        else if (this.equals(IN))
            return OUT;
        else
            return BOTH;
    }
}
