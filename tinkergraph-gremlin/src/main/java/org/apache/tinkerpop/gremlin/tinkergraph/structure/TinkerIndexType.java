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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

/**
 * Enum for the different types of indices supported by TinkerGraph
 */
public enum TinkerIndexType {
    /**
     * Standard key-based index
     */
    DEFAULT,

    /**
     * Vector index for similarity search
     */
    VECTOR;

    /**
     * Distance functions for vector index.
     */
    public enum Vector {

        COSINE {
            @Override
            public VectorSimilarityFunction toJVectorFunction() {
                return VectorSimilarityFunction.COSINE;
            }

            @Override
            public float distance(final float[] a, final float[] b) {
                float dot = 0, normA = 0, normB = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += a[i] * b[i];
                    normA += a[i] * a[i];
                    normB += b[i] * b[i];
                }
                final float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
                return denom == 0 ? 1.0f : 1.0f - (dot / denom);
            }
        },
        EUCLIDEAN {
            @Override
            public VectorSimilarityFunction toJVectorFunction() {
                return VectorSimilarityFunction.EUCLIDEAN;
            }

            @Override
            public float distance(final float[] a, final float[] b) {
                float sum = 0;
                for (int i = 0; i < a.length; i++) {
                    final float d = a[i] - b[i];
                    sum += d * d;
                }
                return (float) Math.sqrt(sum);
            }
        },
        INNER_PRODUCT {
            @Override
            public VectorSimilarityFunction toJVectorFunction() {
                return VectorSimilarityFunction.DOT_PRODUCT;
            }

            @Override
            public float distance(final float[] a, final float[] b) {
                float dot = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += a[i] * b[i];
                }
                return 1.0f - dot;
            }
        };

        /**
         * Returns the corresponding JVector similarity function for index construction and search.
         */
        public abstract VectorSimilarityFunction toJVectorFunction();

        /**
         * Computes the distance between two vectors. Lower values indicate greater similarity.
         */
        public abstract float distance(final float[] a, final float[] b);
    }
}
