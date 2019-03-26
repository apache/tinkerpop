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
package org.apache.tinkerpop.language.gremlin;

import org.apache.tinkerpop.language.gremlin.common.CommonTraversal;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.strategy.decoration.ExplainStrategy;
import org.apache.tinkerpop.machine.strategy.finalization.CoefficientStrategy;
import org.apache.tinkerpop.machine.strategy.verification.CoefficientVerificationStrategy;
import org.apache.tinkerpop.machine.structure.StructureFactory;
import org.apache.tinkerpop.machine.structure.data.TVertex;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSource<C> implements Cloneable {

    private final Machine machine;
    private Bytecode<C> bytecode;
    private Coefficient<C> coefficient = (Coefficient<C>) LongCoefficient.create();
    private boolean registered = false;

    TraversalSource(final Machine machine) {
        this.machine = machine;
        this.bytecode = new Bytecode<>();
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, CoefficientStrategy.class); // TODO: remove when strategies full integrated
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, CoefficientVerificationStrategy.class);
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, ExplainStrategy.class);
    }

    public TraversalSource<C> withCoefficient(final Class<? extends Coefficient<C>> coefficient) {
        final TraversalSource<C> clone = this.clone();
        clone.bytecode.addUniqueSourceInstruction(Symbols.WITH_COEFFICIENT, coefficient);
        clone.coefficient = BytecodeUtil.getCoefficient(clone.bytecode).get();
        return clone;
    }

    public TraversalSource<C> withProcessor(final Class<? extends ProcessorFactory> processor) {
        final TraversalSource<C> clone = this.clone();
        clone.bytecode.addSourceInstruction(Symbols.WITH_PROCESSOR, processor);
        return clone;
    }

    public TraversalSource<C> withProcessor(final Class<? extends ProcessorFactory> processor, final Map<String, Object> configuration) {
        final TraversalSource<C> clone = this.clone();
        clone.bytecode.addSourceInstruction(Symbols.WITH_PROCESSOR, processor, configuration);
        return clone;
    }

    public TraversalSource<C> withStructure(final Class<? extends StructureFactory> structure) {
        final TraversalSource<C> clone = this.clone();
        clone.bytecode.addSourceInstruction(Symbols.WITH_STRUCTURE, structure);
        return clone;
    }

    public TraversalSource<C> withStrategy(final Class<? extends Strategy> strategy) {
        final TraversalSource<C> clone = this.clone();
        clone.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, strategy);
        return clone;
    }

    // spawn methods

    public <S> Traversal<C, S, S> inject(final S... objects) {
        this.prepareSourceCode();
        final Bytecode<C> bytecode = this.bytecode.clone();
        final Coefficient<C> coefficient = this.coefficient.clone();
        bytecode.addInstruction(coefficient, Symbols.INITIAL, objects);
        return new CommonTraversal<>(this.machine, bytecode, coefficient); // TODO
    }

    public Traversal<C, TVertex, TVertex> V() {
        this.prepareSourceCode();
        final Bytecode<C> bytecode = this.bytecode.clone();
        final Coefficient<C> coefficient = this.coefficient.clone();
        bytecode.addInstruction(coefficient, Symbols.V);
        return new CommonTraversal<>(this.machine, bytecode, coefficient); // TODO
    }

    //

    private final void prepareSourceCode() {
        if (!this.registered) {
            this.registered = true;
            this.bytecode = this.machine.register(this.bytecode);
        }
    }

    public void close() {
        this.machine.unregister(this.bytecode);
    }

    @Override
    public TraversalSource<C> clone() {
        try {
            final TraversalSource<C> clone = (TraversalSource<C>) super.clone();
            clone.registered = false;
            clone.bytecode = this.bytecode.clone();
            clone.coefficient = this.coefficient.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
