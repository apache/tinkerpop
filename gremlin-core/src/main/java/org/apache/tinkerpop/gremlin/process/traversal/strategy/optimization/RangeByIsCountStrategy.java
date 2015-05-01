/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.P;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class RangeByIsCountStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final Map<BiPredicate, Long> RANGE_PREDICATES = new HashMap<BiPredicate, Long>() {{
        //put(Compare.inside, 0L);
        //put(Compare.outside, 1L);
        put(Contains.within, 1L);
        put(Contains.without, 0L);
    }};
    private static final Set<Compare> INCREASED_OFFSET_SCALAR_PREDICATES =
            EnumSet.of(Compare.eq, Compare.neq, Compare.lte, Compare.gt);

    private static final RangeByIsCountStrategy INSTANCE = new RangeByIsCountStrategy();

    private RangeByIsCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final int size = traversal.getSteps().size();
        Step prev = null;
        for (int i = 0; i < size; i++) {
            final Step curr = traversal.getSteps().get(i);
            if (curr instanceof CountGlobalStep && i < size - 1) {
                final Step next = traversal.getSteps().get(i + 1);
                if (next instanceof IsStep && !(prev instanceof RangeGlobalStep)) { // if a RangeStep was provided, assume that the user knows what he's doing
                    final IsStep isStep = (IsStep) next;
                    Long highRange = null;
                    for (P p : (Iterable<P>) isStep.getPredicates()) {
                        final Object value = p.getValue();
                        final BiPredicate predicate = p.getBiPredicate();
                        if (value instanceof Number) {
                            final long highRangeOffset = INCREASED_OFFSET_SCALAR_PREDICATES.contains(predicate) ? 1L : 0L;
                            final Long highRangeCandidate = ((Number) value).longValue() + highRangeOffset;
                            highRange = highRange == null || highRangeCandidate > highRange ? highRangeCandidate : highRange;
                        } else {
                            final Long highRangeOffset = RANGE_PREDICATES.get(predicate);
                            if (value instanceof Collection && highRangeOffset != null) {
                                final Object high = Collections.max((Collection) value);
                                if (high instanceof Number) {
                                    final Long highRangeCandidate = ((Number) high).longValue() + highRangeOffset;
                                    highRange = highRange == null || highRangeCandidate > highRange ? highRangeCandidate : highRange;
                                }
                            }
                        }
                    }
                    if (highRange != null) {
                        TraversalHelper.insertBeforeStep(new RangeGlobalStep<>(traversal, 0L, highRange), curr, traversal);
                        i++;
                    }
                }
            }
            prev = curr;
        }
    }

    public static RangeByIsCountStrategy instance() {
        return INSTANCE;
    }
}
