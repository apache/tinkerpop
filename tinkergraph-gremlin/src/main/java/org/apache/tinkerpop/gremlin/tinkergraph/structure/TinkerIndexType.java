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

import com.github.jelmerk.hnswlib.core.DistanceFunction;
import com.github.jelmerk.hnswlib.core.DistanceFunctions;

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
    public enum Vector implements VectorDistance<float[], Float> {

        COSINE(DistanceFunctions.FLOAT_COSINE_DISTANCE),
        EUCLIDEAN(DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE),
        MANHATTAN(DistanceFunctions.FLOAT_MANHATTAN_DISTANCE),
        INNER_PRODUCT(DistanceFunctions.FLOAT_INNER_PRODUCT),
        BRAY_CURTIS(DistanceFunctions.FLOAT_BRAY_CURTIS_DISTANCE),
        CANBERRA(DistanceFunctions.FLOAT_CANBERRA_DISTANCE),
        CORRELATION(DistanceFunctions.FLOAT_CORRELATION_DISTANCE);

        private final DistanceFunction<float[], Float> distanceFunction;

        Vector(final DistanceFunction<float[], Float> distanceFunction) {
            this.distanceFunction = distanceFunction;
        }

        @Override
        public DistanceFunction<float[], Float> getDistanceFunction() {
            return distanceFunction;
        }
    }

    interface VectorDistance<V, T> {
        DistanceFunction<V, T> getDistanceFunction();
    }
}
