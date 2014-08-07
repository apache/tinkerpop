package com.tinkerpop.gremlin.process.computer.ranking;

import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleVertexProgram implements VertexProgram<Integer> {

    public static final String COUNTER = "counter";

    public void setup(final SideEffects sideEffects) {

    }

    public void execute(final Vertex vertex, final Messenger<Integer> messenger, final SideEffects sideEffects) {
        if (sideEffects.isInitialIteration())
            vertex.property(COUNTER, vertex.<String>value("name").length());
        else
            vertex.property(COUNTER, vertex.<String>value("name").length() + vertex.<Integer>value("counter"));
    }


    public boolean terminate(final SideEffects sideEffects) {
        return sideEffects.getIteration() >= 2;
    }

    public Class<Integer> getMessageClass() {
        return Integer.class;
    }

    public Map<String, KeyType> getElementComputeKeys() {
        return new HashMap<String, KeyType>() {{
            put(COUNTER, KeyType.VARIABLE);
        }};
    }
}