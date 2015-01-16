package com.tinkerpop.gremlin.process.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepPosition {

    public int x;
    public int y;
    public int z;
    public int parentX;

    private StepPosition(final int x, final int y, final int z, final int parentX) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.parentX = parentX;
    }

    public String nextXId() {
        return this.x++ + ":" + this.y + ":" + this.z + ":" + this.parentX;
    }

    public void reset() {
        this.x = 0;
        this.y = 0;
        this.z = 0;
        this.parentX = 0;
    }

    @Override
    public String toString() {
        return this.x + ":" + this.y + ":" + this.z + ":" + this.parentX;
    }

    public static StepPosition of(final int x, final int y, final int z, final int parentX) {
        return new StepPosition(x, y, z, parentX);
    }

    public static StepPosition of() {
        return new StepPosition(0, 0, 0, 0);
    }


}
