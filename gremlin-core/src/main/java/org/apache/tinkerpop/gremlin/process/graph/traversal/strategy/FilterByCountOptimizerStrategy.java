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
package org.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.RangeStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.CountStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiPredicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class FilterByCountOptimizerStrategy extends AbstractTraversalStrategy {

    private static final FilterByCountOptimizerStrategy INSTANCE = new FilterByCountOptimizerStrategy();

    private FilterByCountOptimizerStrategy() {
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
                    if (value instanceof Number) {
                        final long highRangeOffset;
                        final boolean replaceIsStep;
                        if (predicate.equals(Compare.eq) || predicate.equals(Compare.lte) || predicate.equals(Compare.neq)) {
                            highRangeOffset = 1L;
                            replaceIsStep = false;
                        } else if (predicate.equals(Compare.lt)) {
                            highRangeOffset = 0L;
                            replaceIsStep = false;
                        } else if (predicate.equals(Compare.gt)) {
                            highRangeOffset = 1L;
                            replaceIsStep = true;
                        } else if (predicate.equals(Compare.gte)) {
                            highRangeOffset = 0L;
                            replaceIsStep = true;
                        } else continue;

                        final long highRange = ((Number) value).longValue() + highRangeOffset;
                        TraversalHelper.insertBeforeStep(new RangeStep<>(traversal, 0L, highRange), curr, traversal);
                        if (replaceIsStep) {
                            TraversalHelper.replaceStep(isStep, new IsStep<>(traversal, Compare.eq, highRange), traversal);
                        }
                    }
                    if (value instanceof Collection && (predicate.equals(Compare.inside) || predicate.equals(Compare.outside))) {
                        final Iterator iterator = ((Collection) value).iterator();
                        if (iterator.hasNext()) iterator.next();
                        else continue;
                        if (iterator.hasNext()) {
                            final Object high = iterator.next();
                            if (high instanceof Number && !iterator.hasNext()) {
                                final long highRangeOffset = predicate.equals(Compare.inside) ? 0L : 1L;
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

    public static FilterByCountOptimizerStrategy instance() {
        return INSTANCE;
    }
}
