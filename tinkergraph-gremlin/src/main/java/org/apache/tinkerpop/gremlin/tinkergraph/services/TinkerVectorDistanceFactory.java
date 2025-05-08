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
package org.apache.tinkerpop.gremlin.tinkergraph.services;

import com.github.jelmerk.hnswlib.core.DistanceFunction;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIndexType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorDistanceFactory.Params.KEY;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorDistanceFactory.Params.DISTANCE_FUNCTION;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

/**
 * Service to calculate distance between elements in a {@link Path}
 */
public class TinkerVectorDistanceFactory extends TinkerServiceRegistry.TinkerServiceFactory<Path, Float> implements Service<Path, Float> {

    public static final String NAME = "tinker.vector.distance";

    public interface Params {
        /**
         * Specify the key storing the embedding
         */
        String KEY = "key";
        /**
         * The distance function to use.
         */
        String DISTANCE_FUNCTION = "distanceFunction";

        Map DESCRIBE = asMap(
                KEY, "Specify they key storing the embedding for the vector search",
                DISTANCE_FUNCTION, "The distance function to use in the calculation as specified by the TinkerIndexType.Vector name (optional, defaults to \"COSINE\")"
        );
    }

    public TinkerVectorDistanceFactory(final AbstractTinkerGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Streaming;
    }

    @Override
    public Map describeParams() {
        return Params.DESCRIBE;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Streaming);
    }

    @Override
    public Service<Path, Float> createService(final boolean isStart, final Map params) {
        if (isStart) {
            throw new UnsupportedOperationException(Exceptions.cannotStartTraversal);
        }

        if (!params.containsKey(KEY)) {
            throw new IllegalArgumentException("The parameter map must contain the key where the embedding is: " + KEY);
        }

        if (params.containsKey(DISTANCE_FUNCTION)) {
            try {
                TinkerIndexType.Vector.valueOf(params.get(DISTANCE_FUNCTION).toString());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid value for " + DISTANCE_FUNCTION + ". Must be a valid TinkerIndexType.Vector.", ex);
            }
        }

        return this;
    }

    @Override
    public CloseableIterator<Float> execute(final ServiceCallContext ctx, final Traverser.Admin<Path> in, final Map params) {
        final String key = (String) params.get(KEY);

        final TinkerIndexType.Vector vector = TinkerIndexType.Vector.valueOf(
                params.getOrDefault(Params.DISTANCE_FUNCTION, TinkerIndexType.Vector.COSINE).toString());
        final DistanceFunction<float[], Float> distanceFunction = vector.getDistanceFunction();

        final Path path = in.get();
        final int pathLength = path.size();
        final Element start = path.get(0);
        final Element end = path.get(pathLength - 1);

        // if the elements do not have the specified key, then return no results because there's nothing we can
        // calculate distance on
        if (!start.keys().contains(key) || !end.keys().contains(key))
            return CloseableIterator.empty();

        final float[] startEmbedding = start.value(key);
        final float[] endEmbedding = end.value(key);
        return CloseableIterator.of(Collections.singleton(distanceFunction.distance(startEmbedding, endEmbedding)).iterator());
    }

    @Override
    public void close() {}

}

