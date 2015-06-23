/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScopingStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ScopingStrategy INSTANCE = new ScopingStrategy();

    private ScopingStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Set<String> pathLabels = TraversalHelper.getLabels(TraversalHelper.getRootTraversal(traversal));
        traversal.getSteps().stream().forEach(step -> {
            if (step instanceof Scoping) {
                if (step.getPreviousStep() instanceof Scoping)
                    ((Scoping) step).setScope(((Scoping) step.getPreviousStep()).recommendNextScope());
                else if (Scope.global == ((Scoping) step).getScope() && step != traversal.getStartStep()) { // for Match(Where())
                    final Set<String> keys = ((Scoping) step).getScopeKeys();
                    if (!keys.isEmpty() && !keys.stream().filter(pathLabels::contains).findAny().isPresent())
                        ((Scoping) step).setScope(Scope.local);
                }
            }
        });
    }

    public static ScopingStrategy instance() {
        return INSTANCE;
    }
}
