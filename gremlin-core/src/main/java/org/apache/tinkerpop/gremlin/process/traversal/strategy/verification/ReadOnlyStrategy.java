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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.Collections;
import java.util.Set;

/**
 * Detects steps marked with {@link Mutating} and throws an {@link IllegalStateException} if one is found.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.out().addE()                 // throws an VerificationException
 * __.addV()                       // throws an VerificationException
 * __.property(key,value)          // throws an VerificationException
 * __.out().drop()                 // throws an VerificationException
 * </pre>
 */
public final class ReadOnlyStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final ReadOnlyStrategy INSTANCE = new ReadOnlyStrategy();

    private ReadOnlyStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof Mutating)
                throw new VerificationException("The provided traversal has a mutating step and thus is not read only: " + step, traversal);
        }
    }

    @Override
    public Set<Class<? extends VerificationStrategy>> applyPost() {
        return Collections.singleton(ComputerVerificationStrategy.class);
    }

    public static ReadOnlyStrategy instance() {
        return INSTANCE;
    }
}
