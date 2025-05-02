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

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.List;
/**
 * Base class for representing a vector index for performing nearest neighbor searches.
 *
 * @param <T> the type of elements stored in the vector index
 */
public abstract class AbstractTinkerVectorIndex<T extends Element> extends AbstractTinkerIndex<T> {

    protected AbstractTinkerVectorIndex(final AbstractTinkerGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    /**
     * Searches for nearest neighbors in the vector index.
     *
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of elements sorted by distance
     */
    public abstract List<T> findNearest(final String key, final float[] vector, final int k);

    /**
     * Searches for nearest neighbors in the vector index with the default k.
     *
     * @param key    the property key
     * @param vector the query vector
     * @return a list of elements sorted by distance
     */
    public abstract List<T> findNearest(final String key, final float[] vector);

}
