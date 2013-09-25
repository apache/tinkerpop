package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.VertexProgram;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaVertexProgram implements VertexProgram {

    private final Consumer<GraphMemory> setupFunction;
    private final BiConsumer<Vertex, GraphMemory> executeFunction;
    private final Predicate<GraphMemory> terminateFunction;
    private final Map<String, KeyType> computeKeys;

    public LambdaVertexProgram(final Consumer<GraphMemory> setupFunction,
                               final BiConsumer<Vertex, GraphMemory> executeFunction,
                               final Predicate<GraphMemory> terminateFunction,
                               final Map<String, KeyType> computeKeys) {
        this.setupFunction = setupFunction;
        this.executeFunction = executeFunction;
        this.terminateFunction = terminateFunction;
        this.computeKeys = computeKeys;
    }

    public void setup(final GraphMemory graphMemory) {
        this.setupFunction.accept(graphMemory);
    }

    public void execute(final Vertex vertex, final GraphMemory graphMemory) {
        this.executeFunction.accept(vertex, graphMemory);
    }

    public boolean terminate(final GraphMemory graphMemory) {
        return this.terminateFunction.test(graphMemory);
    }

    public Map<String, KeyType> getComputeKeys() {
        return this.computeKeys;
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Consumer<GraphMemory> setupFunction;
        private BiConsumer<Vertex, GraphMemory> executeFunction;
        private Predicate<GraphMemory> terminateFunction;
        private Map<String, KeyType> computeKeys;

        public Builder setup(final Consumer<GraphMemory> setupFunction) {
            this.setupFunction = setupFunction;
            return this;
        }

        public Builder execute(final BiConsumer<Vertex, GraphMemory> executeFunction) {
            this.executeFunction = executeFunction;
            return this;
        }

        public Builder terminate(final Predicate<GraphMemory> terminateFunction) {
            this.terminateFunction = terminateFunction;
            return this;
        }

        public Builder computeKeys(final Map<String, KeyType> computeKeys) {
            this.computeKeys = computeKeys;
            return this;
        }

        public LambdaVertexProgram build() {
            Objects.requireNonNull(this.setupFunction);
            Objects.requireNonNull(this.executeFunction);
            Objects.requireNonNull(this.terminateFunction);
            Objects.requireNonNull(this.computeKeys);
            return new LambdaVertexProgram(this.setupFunction, this.executeFunction, this.terminateFunction, this.computeKeys);
        }
    }
}
