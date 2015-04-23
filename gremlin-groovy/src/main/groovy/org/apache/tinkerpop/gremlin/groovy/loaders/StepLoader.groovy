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

import org.apache.tinkerpop.gremlin.groovy.function.GComparator
import org.apache.tinkerpop.gremlin.groovy.function.GFunction
import org.apache.tinkerpop.gremlin.groovy.function.GSupplier
import org.apache.tinkerpop.gremlin.groovy.function.GUnaryOperator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class StepLoader {

    public static void load() {

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        GraphTraversal.metaClass.by = { final Closure closure ->
            return ((GraphTraversal) delegate).by(1 == closure.getMaximumNumberOfParameters() ? new GFunction(closure) : new GComparator(closure));
        }

        GraphTraversalSource.metaClass.withSack = { final Closure closure ->
            return ((GraphTraversalSource) delegate).withSack(new GSupplier(closure));
        }

        GraphTraversalSource.metaClass.withSack = { final Closure closure, final Closure operator ->
            return ((GraphTraversalSource) delegate).withSack(new GSupplier(closure), new GUnaryOperator(operator));
        }

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSack = { final Closure closure ->
            return ((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSack(new GSupplier(closure));
        }

        GraphTraversalSource.GraphTraversalSourceStub.metaClass.withSack = {
            final Closure closure, final Closure operator ->
                return ((GraphTraversalSource.GraphTraversalSourceStub) delegate).withSack(new GSupplier(closure), new GUnaryOperator(operator));
        }
    }
}
