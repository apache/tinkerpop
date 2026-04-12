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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DeclarativeMatchVerificationStrategyTest {

    @Test
    public void shouldThrowWhenMatchIsTerminalStep() {
        final Traversal.Admin<?, ?> traversal = __.<Object>start().match("MATCH (n) RETURN n").asAdmin();
        final DefaultTraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(DeclarativeMatchVerificationStrategy.instance());
        traversal.setStrategies(strategies);

        try {
            traversal.applyStrategies();
            fail("The strategy should not allow match() as a terminal step");
        } catch (VerificationException ex) {
            assertThat(ex.getMessage(), containsString("match() cannot be a terminal step"));
            assertThat(ex.getMessage(), containsString("select()"));
        }
    }

    @Test
    public void shouldPassWhenMatchIsNotTerminalStep() {
        final Traversal.Admin<?, ?> traversal = __.<Object>start().match("MATCH (n) RETURN n").select("n").asAdmin();
        final DefaultTraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(DeclarativeMatchVerificationStrategy.instance());
        traversal.setStrategies(strategies);

        // should not throw
        traversal.applyStrategies();
    }
}
