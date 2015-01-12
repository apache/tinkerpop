package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
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
public class LambdaVertexProgram<M extends Serializable> extends StaticVertexProgram<M> {

    private static final Set<MessageScope> MESSAGE_SCOPES = new HashSet<>(Arrays.asList(MessageScope.Global.instance()));

    private static final String SETUP_LAMBDA = "gremlin.lambdaVertexProgram.setupLambda";
    private static final String EXECUTE_LAMBDA = "gremlin.lambdaVertexProgram.executeLambda";
    private static final String TERMINATE_LAMBDA = "gremlin.lambdaVertexProgram.terminateLambda";
    private static final String ELEMENT_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.elementComputeKeys";
    private static final String MEMORY_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.memoryComputeKeys";

    private LambdaHolder<Consumer<Memory>> setupLambdaHolder;
    private Consumer<Memory> setupLambda;
    private LambdaHolder<TriConsumer<Vertex, Messenger<M>, Memory>> executeLambdaHolder;
    private TriConsumer<Vertex, Messenger<M>, Memory> executeLambda;
    private LambdaHolder<Predicate<Memory>> terminateLambdaHolder;
    private Predicate<Memory> terminateLambda;
    private Set<String> elementComputeKeys;
    private Set<String> memoryComputeKeys;

    private LambdaVertexProgram() {
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.setupLambdaHolder = LambdaHolder.loadState(configuration, SETUP_LAMBDA);
        this.executeLambdaHolder = LambdaHolder.loadState(configuration, EXECUTE_LAMBDA);
        this.terminateLambdaHolder = LambdaHolder.loadState(configuration, TERMINATE_LAMBDA);
        this.setupLambda = null == this.setupLambdaHolder ? s -> {
        } : this.setupLambdaHolder.get();
        this.executeLambda = null == this.executeLambdaHolder ? (v, m, s) -> {
        } : this.executeLambdaHolder.get();
        this.terminateLambda = null == this.terminateLambdaHolder ? s -> true : this.terminateLambdaHolder.get();

        try {
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
        super.storeState(configuration);
        if (null != this.setupLambdaHolder)
            this.setupLambdaHolder.storeState(configuration);
        if (null != this.executeLambdaHolder)
            this.executeLambdaHolder.storeState(configuration);
        if (null != this.terminateLambdaHolder)
            this.terminateLambdaHolder.storeState(configuration);

        try {
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

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(LambdaVertexProgram.class);
        }

        public Builder setup(final Consumer<Memory> setupLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, SETUP_LAMBDA, setupLambda);
            return this;
        }

        public Builder setup(final Class<? extends Consumer<Memory>> setupClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, SETUP_LAMBDA, setupClass);
            return this;
        }

        public Builder setup(final String scriptEngine, final String setupScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, SETUP_LAMBDA, new String[]{scriptEngine, setupScript});
            return this;
        }

        public Builder setup(final String setupScript) {
            return setup(GREMLIN_GROOVY, setupScript);
        }

        ///////

        public Builder execute(final TriConsumer<Vertex, Messenger, Memory> executeLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, EXECUTE_LAMBDA, executeLambda);
            return this;
        }

        public Builder execute(final Class<? extends TriConsumer<Vertex, Messenger, Memory>> executeClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, EXECUTE_LAMBDA, executeClass);
            return this;
        }

        public Builder execute(final String scriptEngine, final String executeScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, EXECUTE_LAMBDA, new String[]{scriptEngine, executeScript});
            return this;
        }

        public Builder execute(final String setupScript) {
            return execute(GREMLIN_GROOVY, setupScript);
        }

        ///////

        public Builder terminate(final Predicate<Memory> terminateLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, TERMINATE_LAMBDA, terminateLambda);
            return this;
        }

        public Builder terminate(final Class<? extends Predicate<Memory>> terminateClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, TERMINATE_LAMBDA, terminateClass);
            return this;
        }

        public Builder terminate(final String scriptEngine, final String terminateScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, TERMINATE_LAMBDA, new String[]{scriptEngine, terminateScript});
            return this;
        }

        public Builder terminate(final String setupScript) {
            return terminate(GREMLIN_GROOVY, setupScript);
        }

        ///////

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