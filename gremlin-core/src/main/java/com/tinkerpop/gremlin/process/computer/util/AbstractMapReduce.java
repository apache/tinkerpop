package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    @Override
    public MapReduce<MK, MV, RK, RV, R> clone() throws CloneNotSupportedException {
        return (MapReduce<MK, MV, RK, RV, R>) super.clone();
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
    }
}
