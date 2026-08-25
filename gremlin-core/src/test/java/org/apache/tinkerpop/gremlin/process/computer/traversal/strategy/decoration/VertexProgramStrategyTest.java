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
package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class VertexProgramStrategyTest {

    private static boolean providerGraphComputerInitialized = false;
    private static boolean invalidGraphComputerInitialized = false;

    @Test
    public void shouldCreateStrategyForDefaultGraphComputer() {
        create(GraphComputer.class.getName());
    }

    @Test
    public void shouldCreateStrategyForProviderGraphComputer() {
        create(ProviderGraphComputer.class.getName());

        assertThat(providerGraphComputerInitialized, is(false));
    }

    @Test
    public void shouldRejectInvalidGraphComputerWithoutInitializingIt() {
        assertThrows(IllegalArgumentException.class, () -> create(InvalidGraphComputer.class.getName()));
        assertThat(invalidGraphComputerInitialized, is(false));
    }

    private static VertexProgramStrategy create(final String graphComputer) {
        return VertexProgramStrategy.create(new MapConfiguration(
                Collections.singletonMap(VertexProgramStrategy.GRAPH_COMPUTER, graphComputer)));
    }

    public static final class ProviderGraphComputer implements GraphComputer {
        static {
            providerGraphComputerInitialized = true;
        }

        @Override
        public GraphComputer result(final ResultGraph resultGraph) {
            return this;
        }

        @Override
        public GraphComputer persist(final Persist persist) {
            return this;
        }

        @Override
        public GraphComputer program(final VertexProgram vertexProgram) {
            return this;
        }

        @Override
        public GraphComputer mapReduce(final MapReduce mapReduce) {
            return this;
        }

        @Override
        public GraphComputer workers(final int workers) {
            return this;
        }

        @Override
        public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
            return this;
        }

        @Override
        public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
            return this;
        }

        @Override
        public GraphComputer vertexProperties(final Traversal<Vertex, ? extends Property<?>> vertexPropertyFilter) {
            return this;
        }

        @Override
        public Future<ComputerResult> submit() {
            return new CompletableFuture<>();
        }
    }

    private static final class InvalidGraphComputer {
        static {
            invalidGraphComputerInitialized = true;
        }
    }
}
