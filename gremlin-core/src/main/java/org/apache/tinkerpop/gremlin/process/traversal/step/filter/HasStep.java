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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder {   // TODO: make final when graph strategies are fixed up

    private final List<HasContainer> hasContainers;

    public HasStep(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        for (final HasContainer hasContainer : hasContainers) {
            this.hasContainers.add(hasContainer);
        }
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return HasContainer.testAll(traverser.get(), this.hasContainers);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return this.hasContainers;
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
