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
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaRestrictionStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final LambdaRestrictionStrategy INSTANCE = new LambdaRestrictionStrategy();

    private LambdaRestrictionStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal instanceof LambdaHolder)
            throw new IllegalStateException("The provided traversal is a lambda traversal: " + traversal);
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof LambdaHolder)
                throw new IllegalStateException("The provided traversal contains a lambda step: " + step);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> localTraversal : ((TraversalParent) step).getLocalChildren()) {  // this is because the lambda traversal do not have strategies
                    if (localTraversal instanceof LambdaHolder)
                        throw new IllegalStateException("The provided traversal contains a lambda traversal: " + localTraversal);
                }
                for (final Traversal.Admin<?, ?> globalTraversal : ((TraversalParent) step).getGlobalChildren()) {  // this is because the lambda traversal do not have strategies
                    if (globalTraversal instanceof LambdaHolder)
                        throw new IllegalStateException("The provided traversal contains a lambda traversal: " + globalTraversal);
                }
            }
        }
    }

    public static LambdaRestrictionStrategy instance() {
        return INSTANCE;
    }
}
