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
package org.apache.tinkerpop.gremlin.groovy.loaders

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.function.Supplier
import java.util.function.UnaryOperator

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    static void load() {

        GraphTraversal.metaClass.by = { final Closure closure ->
            return ((GraphTraversal) delegate).by(1 == closure.getMaximumNumberOfParameters() ? closure as Function : closure as Comparator);
        }

        TraversalSource.metaClass.withSideEffect = {
            final String key, final Closure initialValue, final Closure reducer ->
                return ((TraversalSource) delegate).withSideEffect(key, initialValue as Supplier, reducer as BinaryOperator);
        }

        TraversalSource.metaClass.withSideEffect = {
            final String key, final Closure initialValue, final BinaryOperator reducer ->
                return ((TraversalSource) delegate).withSideEffect(key, initialValue as Supplier, reducer);
        }

        TraversalSource.metaClass.withSideEffect = { final String key, final Closure object ->
            return ((TraversalSource) delegate).withSideEffect(key, object as Supplier);
        }

        TraversalSource.metaClass.withSack = { final Closure closure ->
            return ((TraversalSource) delegate).withSack(closure as Supplier);
        }

        TraversalSource.metaClass.withSack = { final Closure closure, final Closure splitOrMergeOperator ->
            return ((TraversalSource) delegate).withSack(closure as Supplier, splitOrMergeOperator.getMaximumNumberOfParameters() == 1 ? splitOrMergeOperator as UnaryOperator : splitOrMergeOperator as BinaryOperator);
        }

        TraversalSource.metaClass.withSack = {
            final Closure closure, final Closure splitOperator, final Closure mergeOperator ->
                return ((TraversalSource) delegate).withSack(closure as Supplier, splitOperator as UnaryOperator, mergeOperator as BinaryOperator);
        }

        /**
         * Allows for a mix of arguments which may be either {@code TraversalStrategy} object or a
         * {@code Class<TraversalStrategy>}. If the latter, then the class must be able to be instantiated by the
         * common convention of {@code instance()}.
         */
        TraversalSource.metaClass.withStrategies = { Object... traversalStrategyClassOrInstances ->
            def instances = traversalStrategyClassOrInstances.collect {
                if (it instanceof TraversalStrategy) {
                    return it
                } else if (it instanceof Class<?>) {
                    def inst = it.metaClass.respondsTo(it, "instance") ? it."instance"() : null
                    if (null == inst) throw new IllegalArgumentException("${it.name} missing a static 'instance()' method")
                    return inst
                }
            }

            return ((TraversalSource) delegate).withStrategies(*instances)
        }
    }
}
