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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier

import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.function.Supplier
import java.util.function.UnaryOperator

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public static void load() {

        GraphTraversal.metaClass.by = { final Closure closure ->
            return ((GraphTraversal) delegate).by(1 == closure.getMaximumNumberOfParameters() ? closure as Function : closure as Comparator);
        }

        GraphTraversalSource.metaClass.withSideEffect = { final String key, final Object object ->
            return ((GraphTraversalSource) delegate).withSideEffect(key, object instanceof Closure ? ((Closure) object) as Supplier : new ConstantSupplier<>(object));
        }

        GraphTraversalSource.metaClass.withSack = { final Closure closure ->
            return ((GraphTraversalSource) delegate).withSack(closure as Supplier);
        }

        GraphTraversalSource.metaClass.withSack = { final Closure closure, final Closure splitOrMergeOperator ->
            return ((GraphTraversalSource) delegate).withSack(closure as Supplier, splitOrMergeOperator.getMaximumNumberOfParameters() == 1 ? splitOrMergeOperator as UnaryOperator : splitOrMergeOperator as BinaryOperator);
        }

        GraphTraversalSource.metaClass.withSack = {
            final Closure closure, final Closure splitOperator, final Closure mergeOperator ->
                return ((GraphTraversalSource) delegate).withSack(closure as Supplier, splitOperator as UnaryOperator, mergeOperator as BinaryOperator);
        }

        ///////////////////

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSideEffect = {
            final String key, final Object object ->
                return (((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSideEffect(key, object instanceof Closure ? ((Closure) object) as Supplier : new ConstantSupplier<>(object)));
        }

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSack = { final Closure closure ->
            return ((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSack(closure as Supplier);
        }

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSack = {
            final Closure closure, final Closure splitOrMergeOperator ->
                return ((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSack(closure as Supplier, splitOrMergeOperator.getMaximumNumberOfParameters() == 1 ? splitOrMergeOperator as UnaryOperator : splitOrMergeOperator as BinaryOperator);
        }

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSack = {
            final Closure closure, final Closure splitOperator, Closure mergeOperator ->
                return ((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSack(closure as Supplier, splitOperator as UnaryOperator, mergeOperator as BinaryOperator);
        }
    }
}
