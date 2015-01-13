package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StaticMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    @Override
    public MapReduce<MK, MV, RK, RV, R> clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this);
    }
}
