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
package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.IsStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.CountStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class RangeByIsCountStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final Set<Compare> RANGE_PREDICATES = EnumSet.of(Compare.inside, Compare.outside);
    private static final Set<Compare> INCREASED_OFFSET_PREDICATES =
            EnumSet.of(Compare.eq, Compare.neq, Compare.lte, Compare.gt, Compare.outside);

    private static final RangeByIsCountStrategy INSTANCE = new RangeByIsCountStrategy();

    private RangeByIsCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        final int size = traversal.getSteps().size();
        Step prev = null;
        for (int i = 0; i < size; i++) {
            final Step curr = traversal.getSteps().get(i);
            if (curr instanceof CountStep && i < size - 1) {
                final Step next = traversal.getSteps().get(i + 1);
                if (next instanceof IsStep && !(prev instanceof RangeStep)) { // if a RangeStep was provided, assume that the user knows what he's doing
                    final IsStep isStep = (IsStep) next;
                    final Object value = isStep.getValue();
                    final BiPredicate predicate = isStep.getPredicate();
                    final long highRangeOffset = INCREASED_OFFSET_PREDICATES.contains(predicate) ? 1L : 0L;
                    if (value instanceof Number) {
                        final long highRange = ((Number) value).longValue() + highRangeOffset;
                        TraversalHelper.insertBeforeStep(new RangeStep<>(traversal, 0L, highRange), curr, traversal);
                    }
                    if (value instanceof Collection && RANGE_PREDICATES.contains(predicate)) {
                        final Iterator iterator = ((Collection) value).iterator();
                        if (iterator.hasNext()) iterator.next();
                        else continue;
                        if (iterator.hasNext()) {
                            final Object high = iterator.next();
                            if (high instanceof Number && !iterator.hasNext()) {
                                final long highRange = ((Number) high).longValue() + highRangeOffset;
                                TraversalHelper.insertBeforeStep(new RangeStep<>(traversal, 0L, highRange), curr, traversal);
                            }
                        }
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
