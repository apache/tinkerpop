/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public class TranslationStrategyProcessTest extends AbstractGremlinProcessTest {
    private static final Logger logger = LoggerFactory.getLogger(TranslationStrategyProcessTest.class);

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotHaveAnonymousTraversalMixups() throws Exception {
        if (!g.getStrategies().getStrategy(TranslationStrategy.class).isPresent()) {
            logger.debug("No " + TranslationStrategy.class.getSimpleName() + " is registered and thus, skipping test.");
            return;
        }

        final GraphTraversalSource a = g;
        final GraphTraversalSource b = g.withoutStrategies(TranslationStrategy.class);

        assert a.getStrategies().getStrategy(TranslationStrategy.class).isPresent();
        assert !b.getStrategies().getStrategy(TranslationStrategy.class).isPresent();

        assertEquals(6l, a.V().out().count().next().longValue());
        assertEquals(6l, b.V().out().count().next().longValue());

        assert a.getStrategies().getStrategy(TranslationStrategy.class).isPresent();
        assert !b.getStrategies().getStrategy(TranslationStrategy.class).isPresent();

        assertEquals(2l, a.V().repeat(__.out()).times(2).count().next().longValue());
        assertEquals(2l, b.V().repeat(__.out()).times(2).count().next().longValue());

        assert a.getStrategies().getStrategy(TranslationStrategy.class).isPresent();
        assert !b.getStrategies().getStrategy(TranslationStrategy.class).isPresent();

    }

}
