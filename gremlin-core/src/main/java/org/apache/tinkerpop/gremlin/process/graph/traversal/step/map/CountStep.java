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
package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.step.Reducing;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CountStep<S> extends ReducingBarrierStep<S, Long> implements Reducing<Long, Traverser<S>> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.BULK);

    public CountStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>(0L));
        this.setBiFunction(CountBiFunction.<S>instance());
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public Reducer<Long, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true);
    }

    /////

    private static class CountBiFunction<S> implements BiFunction<Long, Traverser<S>, Long>, Serializable {

        private static final CountBiFunction INSTANCE = new CountBiFunction();

        private CountBiFunction() {

        }

        @Override
        public Long apply(final Long mutatingSeed, final Traverser<S> traverser) {
            return mutatingSeed + traverser.bulk();
        }

        public final static <S> CountBiFunction<S> instance() {
            return INSTANCE;
        }
    }
}
