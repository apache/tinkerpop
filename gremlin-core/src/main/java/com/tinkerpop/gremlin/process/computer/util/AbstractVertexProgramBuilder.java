package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractVertexProgramBuilder<B extends VertexProgram.Builder> implements VertexProgram.Builder {

    public static final String GREMLIN_GROOVY = "gremlin-groovy";

    protected final BaseConfiguration configuration = new BaseConfiguration();

    public AbstractVertexProgramBuilder() {
    }

    public AbstractVertexProgramBuilder(final Class<? extends VertexProgram> vertexProgramClass) {
        this.configuration.setProperty(VertexProgram.VERTEX_PROGRAM, vertexProgramClass.getName());
    }

    /*@Override
    public B graph(final Graph graph) {
        this.configuration.setProperty(Graph.GRAPH, graph.getClass().getName());
        return (B) this;
    }*/

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
