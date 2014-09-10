package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphVariables implements Graph.Variables {

    private final Node graphVariableNode;
    private final Neo4jGraph graph;
    public static final Label GRAPH_VARIABLE_LABEL = DynamicLabel.label("graphVariable");

    protected Neo4jGraphVariables(final Node graphVariableNode, final Neo4jGraph graph) {
        this.graphVariableNode = graphVariableNode;
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.graphVariableNode.getPropertyKeys()) {
            if (!key.equals(GRAPH_VARIABLE_LABEL.name())) {
                keys.add(key);
            }
        }
        return keys;
    }

    @Override
    public <R> Optional<R> get(final String key) {
        this.graph.tx().readWrite();
        return this.graphVariableNode.hasProperty(key) ?
                Optional.of((R) this.graphVariableNode.getProperty(key)) :
                Optional.<R>empty();
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.graph.tx().readWrite();
        try {
            this.graphVariableNode.setProperty(key, value);
        } catch(final IllegalArgumentException e) {
           throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        }
    }

    @Override
    public void remove(final String key) {
        this.graph.tx().readWrite();
        if (this.graphVariableNode.hasProperty(key))
            this.graphVariableNode.removeProperty(key);
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
