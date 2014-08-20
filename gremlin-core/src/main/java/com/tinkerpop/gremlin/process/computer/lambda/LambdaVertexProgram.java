package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SPredicate;
import com.tinkerpop.gremlin.util.function.STriConsumer;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaVertexProgram<M extends Serializable> implements VertexProgram<M> {

    private static final String SETUP_LAMBDA_KEY = "gremlin.lambdaVertexProgram.setupLambdaKey";
    private static final String EXECUTE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.executeLambdaKey";
    private static final String TERMINATE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.terminateLambdaKey";
    private static final String ELEMENT_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.elementComputeKeys";
    private static final String MEMORY_KEYS = "gremlin.lambdaVertexProgram.memoryKeys";

    private SConsumer<Memory> setupLambda;
    private STriConsumer<Vertex, Messenger<M>, Memory> executeLambda;
    private SPredicate<Memory> terminateLambda;
    private Map<String, KeyType> elementComputeKeys;
    private Set<String> memoryKeys;

    private LambdaVertexProgram() {
    }

    public void loadState(final Configuration configuration) {
        try {
            this.setupLambda = configuration.containsKey(SETUP_LAMBDA_KEY) ?
                    VertexProgramHelper.deserialize(configuration, SETUP_LAMBDA_KEY) : s -> {
            };
            this.executeLambda = configuration.containsKey(EXECUTE_LAMBDA_KEY) ?
                    VertexProgramHelper.deserialize(configuration, EXECUTE_LAMBDA_KEY) : (v, m, s) -> {
            };
            this.terminateLambda = configuration.containsKey(TERMINATE_LAMBDA_KEY) ?
                    VertexProgramHelper.deserialize(configuration, TERMINATE_LAMBDA_KEY) : s -> true;
            this.elementComputeKeys = configuration.containsKey(ELEMENT_COMPUTE_KEYS) ?
                    VertexProgramHelper.deserialize(configuration, ELEMENT_COMPUTE_KEYS) : Collections.emptyMap();
            this.memoryKeys = configuration.containsKey(MEMORY_KEYS) ?
                    VertexProgramHelper.deserialize(configuration, MEMORY_KEYS) : Collections.emptySet();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, this.getClass().getName());
        try {
            VertexProgramHelper.serialize(this.setupLambda, configuration, SETUP_LAMBDA_KEY);
            VertexProgramHelper.serialize(this.executeLambda, configuration, EXECUTE_LAMBDA_KEY);
            VertexProgramHelper.serialize(this.terminateLambda, configuration, TERMINATE_LAMBDA_KEY);
            VertexProgramHelper.serialize(this.elementComputeKeys, configuration, ELEMENT_COMPUTE_KEYS);
            VertexProgramHelper.serialize(this.memoryKeys, configuration, MEMORY_KEYS);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void setup(final Memory memory) {
        this.setupLambda.accept(memory);
    }


    public void execute(final Vertex vertex, final Messenger<M> messenger, final Memory memory) {
        this.executeLambda.accept(vertex, messenger, memory);
    }

    public boolean terminate(final Memory memory) {
        return this.terminateLambda.test(memory);
    }

    public Map<String, KeyType> getElementComputeKeys() {
        return this.elementComputeKeys;
    }

    public Set<String> getMemoryComputeKeys() {
        return this.memoryKeys;
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Builder> {


        private Builder() {
            super(LambdaVertexProgram.class);
        }

        public Builder setup(final SConsumer<Memory> setupLambda) {
            try {
                VertexProgramHelper.serialize(setupLambda, configuration, SETUP_LAMBDA_KEY);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder execute(final STriConsumer<Vertex, Messenger, Memory> executeLambda) {
            try {
                VertexProgramHelper.serialize(executeLambda, configuration, EXECUTE_LAMBDA_KEY);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder terminate(final SPredicate<Memory> terminateLambda) {
            try {
                VertexProgramHelper.serialize(terminateLambda, configuration, TERMINATE_LAMBDA_KEY);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder elementComputeKeys(final Object... elementComputeKeys) {
            try {
                final Map<String, KeyType> map = new HashMap<>();
                for (int i = 0; i < elementComputeKeys.length; i = i + 2) {
                    map.put((String) elementComputeKeys[0], (KeyType) elementComputeKeys[1]);
                }
                VertexProgramHelper.serialize(map, configuration, ELEMENT_COMPUTE_KEYS);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder memoryComputeKeys(final Set<String> memoryComputeKeys) {
            try {
                VertexProgramHelper.serialize(memoryComputeKeys, configuration, MEMORY_KEYS);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}