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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.coerceTo;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.mul;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SumGlobalStep<S extends Number> extends ReducingBarrierStep<S, S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    );

    public SumGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setReducingBiOperator((BinaryOperator) Operator.sum);
    }

    /**
     * Advances the starts until a non-null value is found or simply returns {@code null}. In this way, an all
     * {@code null} stream will result in {@code null}.
     */
    @Override
    protected S generateSeedFromStarts() {
        S s = null;
        while (starts.hasNext() && null == s) {
            s = projectTraverser(this.starts.next());
        }

        return s;
    }

    @Override
    public void processAllStarts() {
        if (this.starts.hasNext())
            super.processAllStarts();
    }

    @Override
    public S projectTraverser(final Traverser.Admin<S> traverser) {
        final S value = traverser.get();
        final long bulk = traverser.bulk();

        // force the bulk to the type of the value so that mul doesn't arbitrarily widen the type of the Number
        final Class<? extends Number> clazz = null == value ? Long.class : value.getClass();
        return (S) mul(value, coerceTo(bulk, clazz));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
