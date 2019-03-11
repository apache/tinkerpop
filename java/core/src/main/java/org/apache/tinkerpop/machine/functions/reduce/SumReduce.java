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
package org.apache.tinkerpop.machine.functions.reduce;

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.ReduceFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.NumberHelper;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SumReduce<C, S extends Number> extends AbstractFunction<C, S, S> implements ReduceFunction<C, S, S> {

    public SumReduce(final Coefficient<C> coefficient, final Set<String> labels) {
        super(coefficient, labels);
    }

    @Override
    public S apply(final Traverser<C, S> traverser, final S currentValue) {
        return (S) NumberHelper.add(currentValue, NumberHelper.mul(traverser.object(), traverser.coefficient().count()));
    }

    @Override
    public S merge(final S valueA, final S valueB) {
        return (S) NumberHelper.add(valueA, valueB);
    }

    @Override
    public S getInitialValue() {
        return (S) NumberHelper.add(0, 0);
    }
}
