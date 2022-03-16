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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;

/**
 * Inefficient text search implementation (scan+filter), searches for {@link Property}s by token/regex.
 * Demonstrates a {@link Service.Type#Start} service.
 */
public class TinkerTextSearchFactory<I, R> extends TinkerServiceRegistry.TinkerServiceFactory<I, R> implements Service<I, R> {

    public static final String NAME = "tinker.search";

    public interface Params {
        /**
         * Specify a search term - will be converted to regex via .*(search).*
         */
        String SEARCH = "search";
        /**
         * Directly Specify the regex
         */
        String REGEX = "regex";
        /**
         * Specify the type of Element to search for (optional)
         */
        String TYPE = "type";

        Map DESCRIBE = asMap(
                SEARCH, "Specify a search term - will be converted to regex via .*(search).*",
                REGEX, "Directly specify the regex",
                TYPE, "Specify the type of Element to search for, one of Vertex/Edge/VertexProperty (optional)"
        );

        static Class type(final String type) {
            if (type == null)
                return null;

            switch (type) {
                case "Vertex":
                    return Vertex.class;
                case "Edge":
                    return Edge.class;
                case "VertexProperty":
                    return VertexProperty.class;
                default: throw new IllegalArgumentException("Type must be one of Vertex/Edge/VertexProperty: " + type);
            }
        }
    }

    public TinkerTextSearchFactory(final TinkerGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Start;
    }

    @Override
    public Map describeParams() {
        return Params.DESCRIBE;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        final String regex;
        if (params.containsKey(Params.REGEX)) {
            regex = (String) params.get(Params.REGEX);
        } else if (params.containsKey(Params.SEARCH)) {
            regex = ".*(" + params.get(Params.SEARCH) + ").*";
        } else {
            throw new IllegalStateException("Missing search/regex parameter");
        }
        final Class type = Params.type((String) params.get(Params.TYPE));

        return CloseableIterator.of((Iterator<R>) TinkerHelper.search(graph, regex, Optional.ofNullable(type)));
    }

    @Override
    public void close() {}

}

