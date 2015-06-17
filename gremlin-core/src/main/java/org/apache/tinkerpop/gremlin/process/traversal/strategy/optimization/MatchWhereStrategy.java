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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.exp.XMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MatchWhereStrategy will fold any post-<code>where()</code> step that maintains a traversal constraint into <code>match()</code>.
 * {@link MatchStep} is intelligent with traversal constraint applications and thus, can more efficiently use the constraint of {@link WhereStep}.
 * <p/>
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.match(a,b).where(c)            // is replaced by __.match(a,b,c)
 * __.match(a,b).select().where(c)  // is replaced by __.match(a,b,c).select()
 * </pre>
 */
public final class MatchWhereStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final MatchWhereStrategy INSTANCE = new MatchWhereStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(IdentityRemovalStrategy.class);
    }

    private MatchWhereStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(XMatchStep.class, traversal))
            return;

        // pull out match(as("a").has().has()) to has().has().match() in order to allow vendors to leverages has-containers for index lookups
        TraversalHelper.getStepsOfClass(XMatchStep.class, traversal).forEach(matchStep -> {
            ((XMatchStep<?, ?>) matchStep).getGlobalChildren().stream().collect(Collectors.toList()).forEach(conjunction -> {
                ((XMatchStep<?, ?>.XMatchStartStep) conjunction.getStartStep()).getSelectKey().ifPresent(selectKey -> {
                    if (selectKey.equals(matchStep.getStartKey()) && !conjunction.getSteps().stream()
                            .filter(step -> !(step instanceof XMatchStep.XMatchStartStep) &&
                                    !(step instanceof XMatchStep.XMatchEndStep) &&
                                    !(step instanceof HasStep))
                            .findAny()
                            .isPresent()) {
                        matchStep.removeGlobalChild(conjunction);
                        conjunction.removeStep(0);                                     // remove XMatchStartStep
                        conjunction.removeStep(conjunction.getSteps().size() - 1);    // remove XMatchEndStep
                        TraversalHelper.insertTraversal(matchStep.getPreviousStep(), conjunction, traversal);
                    }
                });
            });
        });
    }

    public static MatchWhereStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
