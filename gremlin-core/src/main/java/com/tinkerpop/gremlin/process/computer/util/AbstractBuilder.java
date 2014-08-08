package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractBuilder<B extends VertexProgram.Builder> implements VertexProgram.Builder {

    protected final Configuration configuration = new BaseConfiguration();

    public AbstractBuilder(final Class<? extends VertexProgram> vertexProgramClass) {
        this.configuration.setProperty(GraphComputer.VERTEX_PROGRAM, vertexProgramClass.getName());
    }

    @Override
    public B configure(final Object... keyValues) {
        VertexProgramHelper.legalConfigurationKeyValueArray(keyValues);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            this.configuration.setProperty((String) keyValues[i], keyValues[i + 1]);
        }
        return (B) this;
    }

    @Override
    public <P extends VertexProgram> P create() {
        return (P) VertexProgram.createVertexProgram(this.configuration);
    }
}
