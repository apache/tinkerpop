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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;
import java.util.Optional;

/**
 * A TraversalSource is responsible for generating a {@link Traversal}. A TraversalSource, once built, can generate any number of {@link Traversal} instances.
 * Each traversal DSL will maintain a corresponding {@link TraversalSource} which specifies the methods which being a "fluent-chaining" of traversal steps.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalSource {

    public TraversalSource.Builder asBuilder();

    public Optional<GraphComputer> getGraphComputer();

    public Optional<Graph> getGraph();

    ////////////////

    public interface Builder<C extends TraversalSource> extends Serializable {

        public Builder engine(final TraversalEngine.Builder engine);

        public Builder with(final TraversalStrategy strategy);

        public Builder without(final Class<? extends TraversalStrategy> strategyClass);

        public C create(final Graph graph);
    }
}
