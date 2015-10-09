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

package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStepHelper {

    private GroupStepHelper() {

    }

    public static <S, E> Traversal.Admin<S, E> convertValueTraversal(final Traversal.Admin<S, E> valueReduceTraversal) {
        if (valueReduceTraversal instanceof ElementValueTraversal ||
                valueReduceTraversal instanceof TokenTraversal ||
                valueReduceTraversal instanceof IdentityTraversal ||
                valueReduceTraversal.getStartStep() instanceof LambdaMapStep && ((LambdaMapStep) valueReduceTraversal.getStartStep()).getMapFunction() instanceof FunctionTraverser) {
            return (Traversal.Admin<S, E>) __.map(valueReduceTraversal).fold();
        } else {
            return valueReduceTraversal;
        }
    }

    public static List<Traversal.Admin<?, ?>> splitOnBarrierStep(final Traversal.Admin<?, ?> valueReduceTraversal) {
        if (TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueReduceTraversal).isPresent()) {
            final Traversal.Admin<?, ?> first = __.identity().asAdmin();
            final Traversal.Admin<?, ?> second = __.identity().asAdmin();
            boolean onSecond = false;
            for (final Step step : valueReduceTraversal.getSteps()) {
                if (step instanceof Barrier)
                    onSecond = true;
                if (onSecond)
                    second.addStep(step.clone());
                else
                    first.addStep(step.clone());
            }
            return Arrays.asList(first, second);
        } else {
            return Arrays.asList(valueReduceTraversal.clone(), __.identity().asAdmin());
        }
    }


    /////////

    public static class GroupMap<S, K, V> extends HashMap<K, Traversal.Admin<S, V>> implements FinalGet<Map<K, V>> {

        private final Map<K, V> map;

        public GroupMap(final Map<K, V> map) {
            this.map = map;
        }

        @Override
        public Map<K, V> getFinal() {
            this.forEach((key, traversal) -> this.map.put(key, traversal.next()));
            return this.map;
        }
    }

    public static class GroupMapSupplier implements Supplier<GroupMap>, Serializable {

        @Override
        public GroupMap get() {
            return new GroupMap<>(new HashMap<>());
        }
    }
}
