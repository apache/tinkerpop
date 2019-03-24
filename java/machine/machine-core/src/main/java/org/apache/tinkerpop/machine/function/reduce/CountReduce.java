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
package org.apache.tinkerpop.machine.function.reduce;

import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountReduce<C, S> extends AbstractFunction<C> implements ReduceFunction<C, S, Long> {

    public CountReduce(final Coefficient<C> coefficient, final Set<String> labels) {
        super(coefficient, labels);
    }

    @Override
    public Long apply(final Traverser<C, S> traverser, final Long currentValue) {
        return currentValue + traverser.coefficient().count();
    }

    @Override
    public Long merge(final Long valueA, final Long valueB) {
        return valueA + valueB;
    }

    @Override
    public Long getInitialValue() {
        return 0L;
    }
}