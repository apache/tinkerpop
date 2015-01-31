package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StaticVertexProgram<M> implements VertexProgram<M> {

    @Override
    public StaticVertexProgram<M> clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
    }

}

