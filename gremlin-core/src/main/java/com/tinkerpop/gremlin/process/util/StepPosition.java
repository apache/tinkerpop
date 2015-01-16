package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepPosition {

    public int xx; // need to have the parent x in there. geez, this is complicated.
    public int x;
    public int y;
    public int z;

    private StepPosition(final int x, final int y, final int z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public String nextXId() {
        return Graph.Hidden.hide(this.x++ + ":" + this.y + ":" + this.z);
    }

    public void reset() {
        this.x = 0;
        this.y = 0;
        this.z = 0;
    }

    @Override
    public String toString() {
        return Graph.Hidden.hide(this.x + ":" + this.y + ":" + this.z);
    }

    public static StepPosition of(final int x, final int y, final int z) {
        return new StepPosition(x, y, z);
    }

    public static StepPosition of() {
        return new StepPosition(0, 0, 0);
    }


}
