package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaVertexProgram<M extends Serializable> implements VertexProgram<M> {

    private static final String SETUP_LAMBDA_KEY = "gremlin.lambdaVertexProgram.setupLambdaKey";
    private static final String EXECUTE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.executeLambdaKey";
    private static final String TERMINATE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.terminateLambdaKey";
    private static final String ELEMENT_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.elementComputeKeys";
    private static final String MEMORY_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.memoryComputeKeys";

    private Consumer<Memory> setupLambda;
    private TriConsumer<Vertex, Messenger<M>, Memory> executeLambda;
    private Predicate<Memory> terminateLambda;
    private Set<String> elementComputeKeys;
    private Set<String> memoryComputeKeys;

    private LambdaVertexProgram() {
    }

    @Override
    public void loadState(final Configuration configuration) {
        try {
            this.setupLambda = configuration.containsKey(SETUP_LAMBDA_KEY) ?
                    (Consumer<Memory>) configuration.getProperty(SETUP_LAMBDA_KEY) : s -> {
            };
            this.executeLambda = configuration.containsKey(EXECUTE_LAMBDA_KEY) ?
                    (TriConsumer<Vertex, Messenger<M>, Memory>) configuration.getProperty(EXECUTE_LAMBDA_KEY) : (v, m, s) -> {
            };
            this.terminateLambda = configuration.containsKey(TERMINATE_LAMBDA_KEY) ?
                    (Predicate<Memory>) configuration.getProperty(TERMINATE_LAMBDA_KEY) : s -> true;
            this.elementComputeKeys = configuration.containsKey(ELEMENT_COMPUTE_KEYS) ?
                    VertexProgramHelper.deserialize(configuration, ELEMENT_COMPUTE_KEYS) : Collections.emptySet();
            this.memoryComputeKeys = configuration.containsKey(MEMORY_COMPUTE_KEYS) ?
                    VertexProgramHelper.deserialize(configuration, MEMORY_COMPUTE_KEYS) : Collections.emptySet();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, this.getClass().getName());
        try {
            configuration.setProperty(SETUP_LAMBDA_KEY, this.setupLambda);
            configuration.setProperty(EXECUTE_LAMBDA_KEY, this.executeLambda);
            configuration.setProperty(TERMINATE_LAMBDA_KEY, this.terminateLambda);
            VertexProgramHelper.serialize(this.elementComputeKeys, configuration, ELEMENT_COMPUTE_KEYS);
            VertexProgramHelper.serialize(this.memoryComputeKeys, configuration, MEMORY_COMPUTE_KEYS);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setup(final Memory memory) {
        this.setupLambda.accept(memory);
    }


    @Override
    public void execute(final Vertex vertex, final Messenger<M> messenger, final Memory memory) {
        this.executeLambda.accept(vertex, messenger, memory);
    }

    @Override
    public boolean terminate(final Memory memory) {
        return this.terminateLambda.test(memory);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return this.elementComputeKeys;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Builder> {


        private Builder() {
            super(LambdaVertexProgram.class);
        }

        public Builder setup(final Consumer<Memory> setupLambda) {
            this.configuration.setProperty(SETUP_LAMBDA_KEY, setupLambda);
            return this;
        }

        public Builder execute(final TriConsumer<Vertex, Messenger, Memory> executeLambda) {
            this.configuration.setProperty(EXECUTE_LAMBDA_KEY, executeLambda);
            return this;
        }

        public Builder terminate(final Predicate<Memory> terminateLambda) {
            this.configuration.setProperty(TERMINATE_LAMBDA_KEY, terminateLambda);
            return this;
        }

        public Builder memoryComputeKeys(final Set<String> memoryComputeKeys) {
            try {
                VertexProgramHelper.serialize(memoryComputeKeys, configuration, MEMORY_COMPUTE_KEYS);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder elementComputeKeys(final Set<String> elementComputeKeys) {
            try {
                VertexProgramHelper.serialize(elementComputeKeys, configuration, ELEMENT_COMPUTE_KEYS);
                return this;
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public Builder memoryComputeKeys(final String... memoryComputeKeys) {
            return this.memoryComputeKeys(new HashSet<>(Arrays.asList(memoryComputeKeys)));
        }

        public Builder elementComputeKeys(final String... elementComputeKeys) {
            return this.elementComputeKeys(new HashSet<>(Arrays.asList(elementComputeKeys)));
        }
    }
}