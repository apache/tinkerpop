package com.tinkerpop.gremlin.process.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepPosition {

    public int x;
    public int y;
    public int z;
    public int parentX;
    public int parentZ;

    private StepPosition(final int x, final int y, final int z, final int parentX, final int parentZ) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.parentX = parentX;
        this.parentZ = parentZ;
    }

    public String nextXId() {
        return this.x++ + ":" + this.y + ":" + this.z + ":" + this.parentX + ":" + this.parentZ;
    }

    public void reset() {
        this.x = 0;
        this.y = 0;
        this.z = 0;
        this.parentX = 0;
        this.parentZ = 0;
    }

    @Override
    public String toString() {
        return this.x + ":" + this.y + ":" + this.z + ":" + this.parentX + ":" + this.parentZ;
    }

    public static StepPosition of(final int x, final int y, final int z, final int parentX, final int parentZ) {
        return new StepPosition(x, y, z, parentX, parentZ);
    }

    public static StepPosition of() {
        return new StepPosition(0, 0, 0, 0, 0);
    }


}
