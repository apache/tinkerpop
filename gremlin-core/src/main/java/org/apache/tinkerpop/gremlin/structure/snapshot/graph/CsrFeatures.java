/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.graph;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.LabelCardinality;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceDefaults;

import java.util.List;

/**
 * The read-only features of a {@link CsrGraph}. The supported data types are those of the snapshot's
 * {@code ValueType}; of the types that {@link Graph.Features.DataTypeFeatures} can describe, these are booleans,
 * bytes, doubles, floats, integers, longs, strings, maps, lists (uniform and mixed) and byte arrays. Other arrays and
 * serializable values are not supported. Multi-properties, meta-properties, null property values and graph variables
 * are supported for reading. Vertex label cardinality, the default vertex label and the cardinality of a property key
 * come from the snapshot.
 */
public final class CsrFeatures implements Graph.Features {

    private final GraphFeatures graphFeatures = new CsrGraphFeatures();
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures = new CsrEdgeFeatures();

    CsrFeatures(final CsrSnapshot snapshot) {
        this.vertexFeatures = new CsrVertexFeatures(snapshot);
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return org.apache.tinkerpop.gremlin.structure.util.StringFactory.featureString(this);
    }

    private static final class CsrGraphFeatures implements GraphFeatures {
        @Override
        public boolean supportsComputer() {
            return true;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsIoWrite() {
            return false;
        }

        @Override
        public VariableFeatures variables() {
            return new CsrVariableFeatures();
        }
    }

    private static final class CsrVertexFeatures implements VertexFeatures {
        private final VertexPropertyFeatures propertyFeatures = new CsrVertexPropertyFeatures();
        private final CsrSnapshot snapshot;
        private final List<String> keys;
        private final SourceDefaults defaults;
        private final VertexProperty.Cardinality defaultCardinality;
        private final LabelCardinality labelCardinality;

        private CsrVertexFeatures(final CsrSnapshot snapshot) {
            this.snapshot = snapshot;
            this.keys = snapshot.vertexPropertyKeys();
            this.defaults = snapshot.sourceDefaults();
            this.defaultCardinality = parseCardinality(defaults.defaultVertexPropertyCardinality());
            this.labelCardinality = parseLabelCardinality(defaults.vertexLabelCardinality());
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            final int code = keys.indexOf(key);
            return code >= 0 && snapshot.isMultiProperty(code) ? VertexProperty.Cardinality.list : defaultCardinality;
        }

        @Override
        public LabelCardinality getLabelCardinality() {
            return labelCardinality;
        }

        @Override
        public String getDefaultLabel() {
            return defaults.defaultVertexLabel();
        }

        @Override
        public boolean supportsAddVertices() {
            return false;
        }

        @Override
        public boolean supportsRemoveVertices() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return true;
        }

        @Override
        public boolean supportsMetaProperties() {
            return true;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public boolean supportsAddProperty() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return propertyFeatures;
        }
    }

    private static final class CsrEdgeFeatures implements EdgeFeatures {
        private final EdgePropertyFeatures propertyFeatures = new CsrEdgePropertyFeatures();

        @Override
        public boolean supportsAddEdges() {
            return false;
        }

        @Override
        public boolean supportsRemoveEdges() {
            return false;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public boolean supportsAddProperty() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return propertyFeatures;
        }
    }

    private static final class CsrVertexPropertyFeatures implements VertexPropertyFeatures {
        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }
    }

    private static final class CsrEdgePropertyFeatures implements EdgePropertyFeatures {
        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }
    }

    private static final class CsrVariableFeatures implements VariableFeatures {
        @Override
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        public boolean supportsByteValues() {
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
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
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
            return true;
        }
    }

    private static VertexProperty.Cardinality parseCardinality(final String name) {
        try {
            return VertexProperty.Cardinality.valueOf(name.toLowerCase());
        } catch (IllegalArgumentException e) {
            return VertexProperty.Cardinality.single;
        }
    }

    private static LabelCardinality parseLabelCardinality(final String name) {
        try {
            return LabelCardinality.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return LabelCardinality.ONE;
        }
    }
}
