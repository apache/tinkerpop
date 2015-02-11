/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tinkerpop.gremlin.structure.util.batch;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class BatchFeatures implements Graph.Features {

    private final Graph.Features baseFeatures;
    private final BatchGraphFeatures graphFeatures = new BatchGraphFeatures();

    private final VertexFeatures vertexFeatures = new VertexFeatures() {
        @Override
        public boolean supportsUserSuppliedIds() {
            // batch loading supports user supplied ids
            return true;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }
    };

    private final EdgeFeatures edgeFeatures = new EdgeFeatures() {
        @Override
        public boolean supportsUserSuppliedIds() {
            // batch loading supports user supplied identifiers
            return true;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    };

    private final EdgePropertyFeatures edgePropertyFeatures = new BatchEdgePropertyFeatures();
    private final VertexPropertyFeatures vertexPropertyFeatures = new BatchVertexPropertyFeatures();

    public BatchFeatures(final Graph.Features baseFeatures) {
        this.baseFeatures = baseFeatures;
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

    class BatchVertexPropertyFeatures extends BatchDataTypeFeature implements VertexPropertyFeatures {
        @Override
        public boolean supportsProperties() {
            return baseFeatures.vertex().properties().supportsProperties();
        }
    }

    class BatchEdgePropertyFeatures extends BatchDataTypeFeature implements EdgePropertyFeatures {
        @Override
        public boolean supportsProperties() {
            return baseFeatures.edge().properties().supportsProperties();
        }
    }

    class BatchGraphFeatures implements GraphFeatures {

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return baseFeatures.graph().supportsPersistence();
        }

        @Override
        public boolean supportsTransactions() {
            // the transaction is true because as a wrapper the BatchGraph will check the features of the
            // underlying graph and not let it fail if the underlying graph does not support tx.
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public VariableFeatures variables() {
            return new BatchVariableFeatures();
        }
    }

    class BatchVariableFeatures extends BatchDataTypeFeature implements VariableFeatures {
        @Override
        public boolean supportsVariables() {
            return baseFeatures.graph().variables().supportsVariables();
        }
    }

    class BatchDataTypeFeature implements DataTypeFeatures {
        @Override
        public boolean supportsBooleanValues() {
            return baseFeatures.graph().variables().supportsBooleanValues();
        }

        @Override
        public boolean supportsDoubleValues() {
            return baseFeatures.graph().variables().supportsDoubleValues();
        }

        @Override
        public boolean supportsFloatValues() {
            return baseFeatures.graph().variables().supportsFloatValues();
        }

        @Override
        public boolean supportsIntegerValues() {
            return baseFeatures.graph().variables().supportsIntegerValues();
        }

        @Override
        public boolean supportsLongValues() {
            return baseFeatures.graph().variables().supportsLongValues();
        }

        @Override
        public boolean supportsMapValues() {
            return baseFeatures.graph().variables().supportsMapValues();
        }

        @Override
        public boolean supportsByteValues() {
            return baseFeatures.graph().variables().supportsByteValues();
        }

        @Override
        public boolean supportsMixedListValues() {
            return baseFeatures.graph().variables().supportsMixedListValues();
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return baseFeatures.graph().variables().supportsBooleanArrayValues();
        }

        @Override
        public boolean supportsByteArrayValues() {
            return baseFeatures.graph().variables().supportsByteArrayValues();
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return baseFeatures.graph().variables().supportsDoubleArrayValues();
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return baseFeatures.graph().variables().supportsFloatArrayValues();
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return baseFeatures.graph().variables().supportsIntegerArrayValues();
        }

        @Override
        public boolean supportsLongArrayValues() {
            return baseFeatures.graph().variables().supportsLongArrayValues();
        }

        @Override
        public boolean supportsSerializableValues() {
            return baseFeatures.graph().variables().supportsSerializableValues();
        }

        @Override
        public boolean supportsStringValues() {
            return baseFeatures.graph().variables().supportsStringValues();
        }

        @Override
        public boolean supportsUniformListValues() {
            return baseFeatures.graph().variables().supportsUniformListValues();
        }
    }
}
