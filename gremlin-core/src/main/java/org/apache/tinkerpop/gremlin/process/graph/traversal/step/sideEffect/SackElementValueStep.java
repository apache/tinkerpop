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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackElementValueStep<S extends Element, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    );

    private BinaryOperator<V> operator;
    private final String propertyKey;

    public SackElementValueStep(final Traversal.Admin traversal, final BinaryOperator<V> operator, final String propertyKey) {
        super(traversal);
        this.operator = operator;
        this.propertyKey = propertyKey;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        traverser.get().iterators().valueIterator(this.propertyKey).forEachRemaining(value -> {
            traverser.sack(this.operator.apply(traverser.sack(), (V) value));
        });
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.operator, this.propertyKey);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
