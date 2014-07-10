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

        @Override
        public VertexAnnotationFeatures annotations() {
            return annotationFeatures;
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
    private final VertexAnnotationFeatures annotationFeatures = new BatchVertexAnnotationFeatures();

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

    class BatchVertexAnnotationFeatures extends BatchDataTypeFeature implements VertexAnnotationFeatures {
        @Override
        public boolean supportsAnnotations() {
            return baseFeatures.vertex().annotations().supportsAnnotations();
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
        public VariableFeatures memory() {
            return new BatchVariableFeatures();
        }
    }

    class BatchVariableFeatures extends BatchDataTypeFeature implements VariableFeatures {
        @Override
        public boolean supportsVariables() {
            return baseFeatures.graph().memory().supportsVariables();
        }
    }

    class BatchDataTypeFeature implements DataTypeFeatures {
        @Override
        public boolean supportsBooleanValues() {
            return baseFeatures.graph().memory().supportsBooleanValues();
        }

        @Override
        public boolean supportsDoubleValues() {
            return baseFeatures.graph().memory().supportsDoubleValues();
        }

        @Override
        public boolean supportsFloatValues() {
            return baseFeatures.graph().memory().supportsFloatValues();
        }

        @Override
        public boolean supportsIntegerValues() {
            return baseFeatures.graph().memory().supportsIntegerValues();
        }

        @Override
        public boolean supportsLongValues() {
            return baseFeatures.graph().memory().supportsLongValues();
        }

        @Override
        public boolean supportsMapValues() {
            return baseFeatures.graph().memory().supportsMapValues();
        }

        @Override
        public boolean supportsByteValues() {
            return baseFeatures.graph().memory().supportsByteValues();
        }

        @Override
        public boolean supportsMixedListValues() {
            return baseFeatures.graph().memory().supportsMixedListValues();
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return baseFeatures.graph().memory().supportsBooleanArrayValues();
        }

        @Override
        public boolean supportsByteArrayValues() {
            return baseFeatures.graph().memory().supportsByteArrayValues();
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return baseFeatures.graph().memory().supportsDoubleArrayValues();
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return baseFeatures.graph().memory().supportsFloatArrayValues();
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return baseFeatures.graph().memory().supportsIntegerArrayValues();
        }

        @Override
        public boolean supportsLongArrayValues() {
            return baseFeatures.graph().memory().supportsLongArrayValues();
        }

        @Override
        public boolean supportsSerializableValues() {
            return baseFeatures.graph().memory().supportsSerializableValues();
        }

        @Override
        public boolean supportsStringValues() {
            return baseFeatures.graph().memory().supportsStringValues();
        }

        @Override
        public boolean supportsUniformListValues() {
            return baseFeatures.graph().memory().supportsUniformListValues();
        }
    }
}
