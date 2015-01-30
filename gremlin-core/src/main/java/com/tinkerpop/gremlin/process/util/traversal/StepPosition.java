package com.tinkerpop.gremlin.process.util.traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepPosition {

    public int x; // step in traversal length
    public int y; // depth in traversal nested tree
    public int z; // breadth in traversal siblings
    public String parentId; // the traversal holder id

    private StepPosition(final int x, final int y, final int z, final String parentId) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.parentId = parentId;
    }

    public StepPosition() {
        this(0, 0, 0, "");
    }

    public String nextXId() {
        return this.x++ + "." + this.y + "." + this.z + "(" + this.parentId + ")";
    }

    @Override
    public String toString() {
        return this.x + "." + this.y + "." + this.z + "(" + this.parentId + ")";
    }
}
