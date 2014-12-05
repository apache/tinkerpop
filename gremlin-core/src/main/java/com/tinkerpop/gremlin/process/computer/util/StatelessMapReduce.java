package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StatelessMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    @Override
    public MapReduce<MK, MV, RK, RV, R> clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
    }
}
