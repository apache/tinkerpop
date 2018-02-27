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
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BinaryOperator;

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
        this.setSeedSupplier(new ConstantSupplier<>((S) Integer.valueOf(0)));
        this.setReducingBiOperator((BinaryOperator) Operator.sum);
    }

    @Override
    public void processAllStarts() {
        if (this.starts.hasNext())
            super.processAllStarts();
    }

    @Override
    public S projectTraverser(final Traverser.Admin<S> traverser) {
        return (S) mul(traverser.get(), traverser.bulk());
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
