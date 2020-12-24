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
package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;

import java.util.Collections;
import java.util.Set;

/**
 * Detects the presence of a {@link VertexProgramStrategy} and throws an {@link IllegalStateException} if it is found.
 *
 * @author Marc de Lignie
 */
public final class VertexProgramRestrictionStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final VertexProgramRestrictionStrategy INSTANCE = new VertexProgramRestrictionStrategy();

    private VertexProgramRestrictionStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getStrategies().toList().contains(VertexProgramStrategy.instance())) {
            throw new VerificationException("The TraversalSource does not allow the use of a GraphComputer", traversal);
        }
    }

    @Override
    public Set<Class<? extends VerificationStrategy>> applyPost() {
        return Collections.singleton(ComputerVerificationStrategy.class);
    }

    public static VertexProgramRestrictionStrategy instance() {
        return INSTANCE;
    }
}
