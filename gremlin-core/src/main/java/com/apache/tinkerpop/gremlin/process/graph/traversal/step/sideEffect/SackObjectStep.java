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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackObjectStep<S, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    );

    private BiFunction<V, S, V> operator;

    public SackObjectStep(final Traversal.Admin traversal, final BiFunction<V, S, V> operator) {
        super(traversal);
        this.operator = operator;
        SackObjectStep.generateConsumer(this);

    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SackObjectStep<S, V> clone() throws CloneNotSupportedException {
        final SackObjectStep<S, V> clone = (SackObjectStep<S, V>) super.clone();
        SackObjectStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S, V> void generateConsumer(final SackObjectStep<S, V> sackObjectStep) {
        sackObjectStep.setConsumer(traverser -> traverser.sack(sackObjectStep.operator.apply(traverser.sack(), traverser.get())));
    }
}
