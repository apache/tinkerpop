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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;

import java.util.List;
import java.util.Map;

/**
 * An interface for common functionality of {@link GroupStep} and {@link GroupSideEffectStep}.
 */
public interface Grouping<S, K, V> {

    /**
     * Determines if the provided traversal is equal to the key traversal that the {@code Grouping} has.
     */
    public Traversal.Admin<S, K> getKeyTraversal();

    /**
     * Determines if the provided traversal is equal to the value traversal that the {@code Grouping} has.
     */
    public Traversal.Admin<S, V> getValueTraversal();

    /**
     * Determines the first (non-local) barrier step in the provided traversal. This method is used by {@link GroupStep}
     * and {@link GroupSideEffectStep} to ultimately determine the reducing bi-operator.
     *
     * @param traversal The traversal to inspect.
     * @return The first non-local barrier step or {@code null} if no such step was found.
     */
    public default Barrier determineBarrierStep(final Traversal.Admin<S, V> traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int ix = 0; ix < steps.size(); ix++) {
            final Step step = steps.get(ix);
            if (step instanceof Barrier && !(step instanceof LocalBarrier)) {
                final Barrier b = (Barrier) step;

                // when profile() is enabled the step needs to be wrapped up with the barrier so that the timer on
                // the ProfileStep is properly triggered
                if (ix < steps.size() - 1 && steps.get(ix + 1) instanceof ProfileStep)
                    return new ProfilingAware.ProfiledBarrier(b, (ProfileStep) steps.get(ix + 1));
                else
                    return b;
            }
        }
        return null;
    }

    public default Traversal.Admin<S, V> convertValueTraversal(final Traversal.Admin<S, V> valueTraversal) {
        if (valueTraversal instanceof ValueTraversal ||
                valueTraversal instanceof TokenTraversal ||
                valueTraversal instanceof IdentityTraversal ||
                valueTraversal instanceof ColumnTraversal ||
                valueTraversal.getStartStep() instanceof LambdaMapStep && ((LambdaMapStep) valueTraversal.getStartStep()).getMapFunction() instanceof FunctionTraverser) {
            return (Traversal.Admin<S, V>) __.map(valueTraversal).fold();
        } else
            return valueTraversal;
    }

    public default Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<S, V> valueTraversal) {
        final Barrier barrierStep = determineBarrierStep(valueTraversal);
        if (barrierStep != null) {
            for (final K key : map.keySet()) {
                valueTraversal.reset();
                barrierStep.addBarrier(map.get(key));
                if (valueTraversal.hasNext())
                    map.put(key, valueTraversal.next());
            }
        }
        return (Map<K, V>) map;
    }
}
