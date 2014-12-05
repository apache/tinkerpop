package com.tinkerpop.gremlin;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A class that defines a simple representation of a schema, so that those graphs that support schema definition
 * can properly prepare their {@link com.tinkerpop.gremlin.structure.Graph} instance before a test is executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Schema {

    private final Set<String> edgeLabels;
    private final Set<String> vertexLabels;
    private final Set<EdgePropertyDefinition> edgePropertyDefinitions;
    private final Set<VertexPropertyDefinition> vertexPropertyDefinitions;
    private final Set<VariablePropertyDefinition> variablePropertyDefinitions;

    private Schema(final Set<String> vertexLabels, final Set<String> edgeLabels,
                   final Set<EdgePropertyDefinition> edgePropertyDefinitions,
                   final Set<VertexPropertyDefinition> vertexPropertyDefinitions,
                   final Set<VariablePropertyDefinition> variablePropertyDefinitions) {
        this.edgeLabels = edgeLabels;
        this.vertexLabels = vertexLabels;
        this.edgePropertyDefinitions = edgePropertyDefinitions;
        this.vertexPropertyDefinitions = vertexPropertyDefinitions;
        this.variablePropertyDefinitions = variablePropertyDefinitions;
    }

    public Set<String> getEdgeLabels() {
        return Collections.unmodifiableSet(edgeLabels);
    }

    public Set<String> getVertexLabels() {
        return Collections.unmodifiableSet(vertexLabels);
    }

    public Set<EdgePropertyDefinition> getEdgePropertyDefinitions() {
        return Collections.unmodifiableSet(edgePropertyDefinitions);
    }

    public Set<VertexPropertyDefinition> getVertexPropertyDefinitions() {
        return Collections.unmodifiableSet(vertexPropertyDefinitions);
    }

    public Set<VariablePropertyDefinition> getVariablePropertyDefinitions() {
        return Collections.unmodifiableSet(variablePropertyDefinitions);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private final Set<String> edgeLabels = new HashSet<>();
        private final Set<String> vertexLabels = new HashSet<>();
        private final Set<EdgePropertyDefinition> edgePropertyDefinitions = new HashSet<>();
        private final Set<VariablePropertyDefinition> variablePropertyDefinitions = new HashSet<>();
        private final Set<VertexPropertyDefinition> vertexPropertyDefinitions = new HashSet<>();

        public Builder addEdgeLabel(final String label) {
            edgeLabels.add(label);
            return this;
        }

        public Builder addVertexLabel(final String label) {
            vertexLabels.add(label);
            return this;
        }

        public Builder addEdgePropertyDefinition(final String name, final Class dataType) {
            edgePropertyDefinitions.add(new EdgePropertyDefinition(name, dataType));
            return this;
        }

        public Builder addVariablePropertyDefinition(final String name, final Class dataType) {
            variablePropertyDefinitions.add(new VariablePropertyDefinition(name, dataType));
            return this;
        }

        public Builder addVertexPropertyDefinition(final String name, final Class dataType, final PropertyDefinition... properties) {
            vertexPropertyDefinitions.add(new VertexPropertyDefinition(name, dataType, properties));
            return this;
        }

        public Schema create() {
            return new Schema(vertexLabels, edgeLabels, edgePropertyDefinitions,
                    vertexPropertyDefinitions, variablePropertyDefinitions);
        }
    }

    public static class EdgePropertyDefinition extends PropertyDefinition {
        public EdgePropertyDefinition(final String name, final Class dataType) {
            super(name, dataType);
        }
    }

    public static class VariablePropertyDefinition extends PropertyDefinition {
        public VariablePropertyDefinition(final String name, final Class dataType) {
            super(name, dataType);
        }
    }

    public static class PropertyDefinition {
        private final String name;
        private final Class dataType;

        public PropertyDefinition(final String name, final Class dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        public String getName() {
            return name;
        }

        public Class getDataType() {
            return dataType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PropertyDefinition that = (PropertyDefinition) o;

            if (!dataType.equals(that.dataType)) return false;
            if (!name.equals(that.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + dataType.hashCode();
            return result;
        }
    }

    public static class VertexPropertyDefinition extends PropertyDefinition {
        private final Set<PropertyDefinition> properties;

        public VertexPropertyDefinition(final String name, final Class dataType, final PropertyDefinition... properties) {
            super(name, dataType);
            this.properties = new HashSet<>(Arrays.asList(properties));
        }

        public Set<PropertyDefinition> getProperties() {
            return Collections.unmodifiableSet(properties);
        }
    }
}
