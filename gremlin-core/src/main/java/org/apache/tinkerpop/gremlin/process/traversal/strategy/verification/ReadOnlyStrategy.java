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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * Detects steps marked with {@link Mutating} and throw an {@link IllegalStateException} if one is found.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ReadOnlyStrategy extends AbstractTraversalStrategy {

    private static final ReadOnlyStrategy INSTANCE = new ReadOnlyStrategy();

    private ReadOnlyStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getSteps().stream().anyMatch(step -> step instanceof Mutating))
            throw new IllegalStateException("The provided traversal has a mutating step and thus is not read only");
    }

    public static ReadOnlyStrategy instance() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategyString(this);
    }
}
