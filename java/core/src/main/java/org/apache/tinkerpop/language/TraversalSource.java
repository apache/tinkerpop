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
package org.apache.tinkerpop.language;

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.compiler.Strategy;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSource<C> {

    private Coefficient<C> coefficient;
    private ProcessorFactory factory;
    private List<Strategy> strategies = new ArrayList<>();


    protected TraversalSource() {
        this.coefficient = (Coefficient<C>) LongCoefficient.create();
    }

    public TraversalSource<C> coefficient(final Coefficient<C> coefficient) {
        this.coefficient = coefficient.clone();
        this.coefficient.unity();
        return this;
    }

    public TraversalSource<C> processor(final Class<? extends ProcessorFactory> processor) {
        try {
            this.factory = processor.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return this;
    }

    public TraversalSource<C> strategy(final Strategy strategy) {
        this.strategies.add(strategy);
        return this;
    }

    public <S> Traversal<C, S, S> inject(final S... objects) {
        final Traversal<C, S, S> traversal = new Traversal<>(this.coefficient, this.factory);
        return traversal.inject(objects);
    }
}
