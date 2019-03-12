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

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategies.CoefficientStrategy;
import org.apache.tinkerpop.machine.strategies.Strategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSource<C> {

    private Bytecode<C> bytecode;

    protected TraversalSource() {
        this.bytecode = new Bytecode<>();
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, CoefficientStrategy.class);
    }

    public TraversalSource<C> withCoefficient(final Class<? extends Coefficient<C>> coefficient) {
        this.bytecode = this.bytecode.clone();
        this.bytecode.addSourceInstruction(Symbols.WITH_COEFFICIENT, coefficient);
        return this;
    }

    public TraversalSource<C> withProcessor(final Class<? extends ProcessorFactory> processor) {
        this.bytecode = this.bytecode.clone();
        this.bytecode.addSourceInstruction(Symbols.WITH_PROCESSOR, processor);
        for (final Strategy strategy : ProcessorFactory.processorStrategies(processor)) {
            this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, strategy.getClass());
        }
        return this;
    }

    public TraversalSource<C> withStrategy(final Class<? extends Strategy> strategy) {
        this.bytecode = this.bytecode.clone();
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, strategy);
        return this;
    }

    public <S> Traversal<C, S, S> inject(final S... objects) {
        return (Traversal<C, S, S>) new Traversal<>(this.bytecode.clone()).inject(objects);
    }
}
