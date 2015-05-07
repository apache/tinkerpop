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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScopingStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ScopingStrategy INSTANCE = new ScopingStrategy();

    private ScopingStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        traversal.getSteps().stream().forEach(step -> {
            if (step.getPreviousStep() instanceof Scoping) {
                if (step instanceof SelectStep)
                    ((SelectStep) step).setScope(((Scoping) step.getPreviousStep()).getScope());
                else if (step instanceof SelectOneStep)
                    ((SelectOneStep) step).setScope(((Scoping) step.getPreviousStep()).getScope());
                else if (step instanceof WhereStep)
                    ((WhereStep) step).setScope(((Scoping) step.getPreviousStep()).getScope());
            }
        });
    }

    public static ScopingStrategy instance() {
        return INSTANCE;
    }
}
