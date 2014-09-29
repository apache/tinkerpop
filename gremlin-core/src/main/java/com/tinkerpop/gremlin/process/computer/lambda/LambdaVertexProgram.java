package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.SupplierType;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaVertexProgram<M extends Serializable> implements VertexProgram<M> {

    private static final String SUPPLIER_TYPE = "gremlin.lambdaVertexProgram.supplierType";
    private static final String SETUP_LAMBDA_KEY = "gremlin.lambdaVertexProgram.setupLambdaKey";
    private static final String EXECUTE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.executeLambdaKey";
    private static final String TERMINATE_LAMBDA_KEY = "gremlin.lambdaVertexProgram.terminateLambdaKey";
    private static final String ELEMENT_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.elementComputeKeys";
    private static final String MEMORY_COMPUTE_KEYS = "gremlin.lambdaVertexProgram.memoryComputeKeys";

    private SupplierType supplierType;
    private Pair<?, Supplier<Consumer<Memory>>> supplierSetupLambda;
    private Consumer<Memory> setupLambda;
    private Pair<?, Supplier<TriConsumer<Vertex, Messenger<M>, Memory>>> supplierExecuteLambda;
    private TriConsumer<Vertex, Messenger<M>, Memory> executeLambda;
    private Pair<?, Supplier<Predicate<Memory>>> supplierTerminateLambda;
    private Predicate<Memory> terminateLambda;
    private Set<String> elementComputeKeys;
    private Set<String> memoryComputeKeys;

    private LambdaVertexProgram() {
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.supplierType = SupplierType.getType(configuration, SUPPLIER_TYPE);
        if (configuration.containsKey(SETUP_LAMBDA_KEY)) {
            this.supplierSetupLambda = this.supplierType.<Consumer<Memory>>get(configuration, SETUP_LAMBDA_KEY);
            this.setupLambda = this.supplierSetupLambda.getValue1().get();
        } else {
            this.setupLambda = s -> {
            };
        }
        if (configuration.containsKey(EXECUTE_LAMBDA_KEY)) {
            this.supplierExecuteLambda = this.supplierType.<TriConsumer<Vertex, Messenger<M>, Memory>>get(configuration, EXECUTE_LAMBDA_KEY);
            this.executeLambda = this.supplierExecuteLambda.getValue1().get();
        } else {
            this.executeLambda = (v, m, s) -> {
            };
        }
        if (configuration.containsKey(TERMINATE_LAMBDA_KEY)) {
            this.supplierTerminateLambda = this.supplierType.<Predicate<Memory>>get(configuration, TERMINATE_LAMBDA_KEY);
            this.terminateLambda = this.supplierTerminateLambda.getValue1().get();
        } else {
            this.terminateLambda = m -> true;
        }

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
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, this.getClass().getName());
        try {
            if (null != this.supplierSetupLambda)
                this.supplierType.set(configuration, SUPPLIER_TYPE, SETUP_LAMBDA_KEY, this.supplierSetupLambda.getValue0());
            if (null != this.supplierExecuteLambda)
                this.supplierType.set(configuration, SUPPLIER_TYPE, EXECUTE_LAMBDA_KEY, this.supplierExecuteLambda.getValue0());
            if (null != this.supplierTerminateLambda)
                this.supplierType.set(configuration, SUPPLIER_TYPE, TERMINATE_LAMBDA_KEY, this.supplierTerminateLambda.getValue0());

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
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE, SETUP_LAMBDA_KEY, (Supplier) () -> setupLambda);
            return this;
        }

        public Builder setup(final String scriptEngine, final String setupScript) {
            SupplierType.SCRIPT.set(this.configuration, SUPPLIER_TYPE, SETUP_LAMBDA_KEY, new String[]{scriptEngine, setupScript});
            return this;
        }

        public Builder execute(final TriConsumer<Vertex, Messenger, Memory> executeLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE, EXECUTE_LAMBDA_KEY, (Supplier) () -> executeLambda);
            return this;
        }

        public Builder execute(final String scriptEngine, final String executeScript) {
            SupplierType.SCRIPT.set(this.configuration, SUPPLIER_TYPE, EXECUTE_LAMBDA_KEY, new String[]{scriptEngine, executeScript});
            return this;
        }


        public Builder terminate(final Predicate<Memory> terminateLambda) {
            SupplierType.OBJECT.set(this.configuration, SUPPLIER_TYPE, TERMINATE_LAMBDA_KEY,(Supplier) () -> terminateLambda);
            return this;
        }

        public Builder terminate(final String scriptEngine, final String executeScript) {
            SupplierType.SCRIPT.set(this.configuration, SUPPLIER_TYPE, TERMINATE_LAMBDA_KEY, new String[]{scriptEngine, executeScript});
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