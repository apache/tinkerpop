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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HasStepGV<S> extends GValueStep<HasStep, S, S> implements HasStepStructure {
    private List<HasContainer> hasContainers;
    private final Parameters parameters = new Parameters();

    public HasStepGV(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal, new HasStep(traversal, GValue.resolveGValuesInHasContainers(hasContainers)));
        this.hasContainers = new ArrayList<>();
        Collections.addAll(this.hasContainers, hasContainers);
    }

    @Override
    public void configure(final Object... keyValues) {
        // all GValues locally stored in the parameters, but resolve them for the concrete step
        this.parameters.set(null, GValue.promoteGValuesInKeyValues(keyValues));
        this.concreteStep.configure(GValue.resolveGValues(keyValues));
    }

    @Override
    public Parameters getParameters() {
        return concreteStep.getParameters();
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void removeHasContainer(final HasContainer hasContainer) {
        // not sure what this means in this context since this object will be different from the one in the concrete
        // step. strategies are the only things that remove HasContainers so since the GValueSteps will all be
        // removed by the time they execute perhaps we don't need to implement this.
        throw new UnsupportedOperationException("Cannot remove HasContainers from HasStepGV");
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        assert(GValue.hasGValueInP(hasContainer.getPredicate()));
        this.hasContainers.add(hasContainer);
        this.concreteStep.addHasContainer(GValue.resolveGValuesInHasContainer(hasContainer));
    }

}
