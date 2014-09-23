package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphVariables implements Graph.Variables {

    private final PropertyContainer graphVariables;
    private final Neo4jGraph graph;

    protected Neo4jGraphVariables(final Neo4jGraph graph) {
        this.graph = graph;
        this.graphVariables = ((GraphDatabaseAPI) this.graph.getBaseGraph()).getDependencyResolver().resolveDependency(NodeManager.class).getGraphProperties();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.graphVariables.getPropertyKeys()) {
            if (!Graph.System.isSystem(key))
                keys.add(key);
        }
        return keys;
    }

    @Override
    public <R> Optional<R> get(final String key) {
        this.graph.tx().readWrite();
        return this.graphVariables.hasProperty(key) ?
                Optional.of((R) this.graphVariables.getProperty(key)) :
                Optional.<R>empty();
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.graph.tx().readWrite();
        try {
            this.graphVariables.setProperty(key, value);
        } catch (final IllegalArgumentException e) {
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        }
    }

    @Override
    public void remove(final String key) {
        this.graph.tx().readWrite();
        if (this.graphVariables.hasProperty(key))
            this.graphVariables.removeProperty(key);
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }

    public static class Neo4jVariableFeatures implements Graph.Features.VariableFeatures {
        @Override
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleValues() {
            return true;
        }

        @Override
        public boolean supportsFloatValues() {
            return true;
        }

        @Override
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        public boolean supportsLongValues() {
            return true;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return true;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return true;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return true;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return true;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return true;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return true;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }
}
