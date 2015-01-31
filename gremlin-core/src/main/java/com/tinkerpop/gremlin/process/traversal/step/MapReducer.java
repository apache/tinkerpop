package com.tinkerpop.gremlin.process.traversal.step;

import com.tinkerpop.gremlin.process.computer.MapReduce;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MapReducer<MK, MV, RK, RV, R> {

    public MapReduce<MK, MV, RK, RV, R> getMapReduce();
}
