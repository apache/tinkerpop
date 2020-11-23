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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder, Configuring {

    private final Parameters parameters = new Parameters();
    private List<HasContainer> hasContainers;

    public HasStep(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        Collections.addAll(this.hasContainers, hasContainers);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        // the generic S is defined as Element but Property can also be used with HasStep so this seems to cause
        // problems with some jdk versions.
        if (traverser.get() instanceof Element)
            return HasContainer.testAll(traverser.get(), this.hasContainers);
        else if (traverser.get() instanceof Property)
            return HasContainer.testAll((Property) traverser.get(), this.hasContainers);
        else
            throw new IllegalStateException(String.format(
                    "Traverser to has() must be of type Property or Element, not %s",
                    traverser.get().getClass().getName()));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void removeHasContainer(final HasContainer hasContainer) {
        this.hasContainers.remove(hasContainer);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    @Override
    public HasStep<S> clone() {
        final HasStep<S> clone = (HasStep<S>) super.clone();
        clone.hasContainers = new ArrayList<>();
        for (final HasContainer hasContainer : this.hasContainers) {
            clone.addHasContainer(hasContainer.clone());
        }
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final HasContainer hasContainer : this.hasContainers) {
            result ^= hasContainer.hashCode();
        }
        return result;
    }
}
