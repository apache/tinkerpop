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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.shuffle;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.junit.Assert.assertEquals;

@RunWith(GremlinProcessRunner.class)
public class SeedStrategyProcessTest extends AbstractGremlinProcessTest {
    private static final SeedStrategy seedStrategy = new SeedStrategy(1235L);

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSeedCoin() {
        final GraphTraversalSource gSeeded = create();
        final List<Object> names = gSeeded.V().order().by("name").values("name").coin(0.31).order().toList();
        repeatAssert(() -> {
            assertEquals(names, gSeeded.V().order().by("name").values("name").coin(0.31).order().toList());
            return null;
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSeedGlobalOrderShuffle() {
        final GraphTraversalSource gSeeded = create();
        final List<Object> names = gSeeded.V().order().by("name").values("name").order().by(shuffle).toList();
        repeatAssert(() -> {
            assertEquals(names, gSeeded.V().order().by("name").values("name").order().by(shuffle).toList());
            return null;
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSeedLocalOrderShuffle() {
        final GraphTraversalSource gSeeded = create();
        final List<Object> names = gSeeded.V().order().by("name").values("name").fold().order(local).by(shuffle).next();
        repeatAssert(() -> {
            assertEquals(names, gSeeded.V().order().by("name").values("name").fold().order(local).by(shuffle).next());
            return null;
        });
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldSeedGlobalSample() {
        final GraphTraversalSource gSeeded = create();
        final List<Object> names = gSeeded.V().order().by("name").values("name").sample(20).toList();
        repeatAssert(() -> {
            assertEquals(names, gSeeded.V().order().by("name").values("name").sample(20).toList());
            return null;
        });
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldSeedLocalSample() {
        final GraphTraversalSource gSeeded = create();
        final List<Object> names = gSeeded.V().order().by("name").values("name").fold().sample(local,20).next();
        repeatAssert(() -> {
            assertEquals(names, gSeeded.V().order().by("name").values("name").fold().sample(local,20).next());
            return null;
        });
    }

    private void repeatAssert(final Supplier<Void> assertion) {
        for (int ix = 0; ix < 128; ix++) {
            assertion.get();
        }
    }

    private GraphTraversalSource create() {
        return g.withStrategies(seedStrategy);
    }
}
