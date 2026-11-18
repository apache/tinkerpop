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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider;

/**
 * Registers {@link TinkerGraph} as {@code gremlin-core}'s {@link DefaultGraphProvider}, discovered with
 * {@link java.util.ServiceLoader} (see {@code META-INF/services}), so that {@code gremlin-core} can open a
 * {@code TinkerGraph} for a deserialized remote {@code Graph} result or for the default {@code subgraph()}
 * side-effect without naming the implementation class directly.
 */
public class TinkerGraphDefaultProvider implements DefaultGraphProvider {

    /**
     * Opens a {@link TinkerGraph} with the given configuration. If the configuration does not name a graph class in
     * {@link Graph#GRAPH}, {@link TinkerGraph} is recorded there, as it would be for a graph opened through
     * {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory}.
     */
    @Override
    public Graph open(final Configuration configuration) {
        if (!configuration.containsKey(Graph.GRAPH))
            configuration.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
        return TinkerGraph.open(configuration);
    }
}
