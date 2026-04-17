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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * Prevents {@link DeclarativeMatchStep} from appearing as the terminal step of any traversal.
 * A terminal {@code match()} produces no usable output because the step emits only
 * {@code Optional.empty()} as the current element; callers must follow {@code match()} with
 * a {@code select()} to access the bound variables.
 *
 * <p>This strategy is registered as a default verification strategy and is applied recursively
 * to the root traversal and all of its anonymous child traversals.</p>
 *
 * @since 4.0.0
 * @example <pre>
 * g.V().match("MATCH (n) RETURN n")              // throws VerificationException
 * g.V().match("MATCH (n) RETURN n").select("n")  // valid
 * </pre>
 */
public final class DeclarativeMatchVerificationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
        implements TraversalStrategy.VerificationStrategy {

    private static final DeclarativeMatchVerificationStrategy INSTANCE = new DeclarativeMatchVerificationStrategy();

    private DeclarativeMatchVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> endStep = traversal.getEndStep();
        if (endStep instanceof DeclarativeMatchStep) {
            throw new VerificationException(
                    "match() cannot be a terminal step — use select() to access bound variables",
                    traversal);
        }
    }

    public static DeclarativeMatchVerificationStrategy instance() {
        return INSTANCE;
    }
}
