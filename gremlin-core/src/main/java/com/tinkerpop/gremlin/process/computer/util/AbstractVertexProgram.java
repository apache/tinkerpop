package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractVertexProgram<M> implements VertexProgram<M> {

    @Override
    public AbstractVertexProgram<M> clone() throws CloneNotSupportedException {
        return (AbstractVertexProgram<M>) super.clone();
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
    }

}

