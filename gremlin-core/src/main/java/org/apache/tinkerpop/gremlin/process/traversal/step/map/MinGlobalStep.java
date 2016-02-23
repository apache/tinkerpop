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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.apache.tinkerpop.gremlin.process.traversal.NumberHelper.min;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MinGlobalStep<S extends Number> extends ReducingBarrierStep<S, S> {

    private static final Double NAN = Double.valueOf(Double.NaN);

    public MinGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>((S) NAN));
        this.setReducingBiOperator(new MinGlobalBiOperator<>());
    }

    @Override
    public S projectTraverser(final Traverser.Admin<S> traverser) {
        return traverser.get();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    /////

    public static class MinGlobalBiOperator<S extends Number> implements BinaryOperator<S>, Serializable {
        @Override
        public S apply(final S mutatingSeed, final S number) {
            return !NAN.equals(mutatingSeed) ? (S) min(mutatingSeed, number) : number;
        }
    }
}