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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.ListFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A map step that returns the cartesian product of the traverser and the provided arguments.
 */
public final class ProductStep<S, E> extends ScalarMapStep<S, List<List<?>>> implements TraversalParent, ListFunction {
    private Traversal.Admin<S, E> valueTraversal;
    private Object parameterItems;

    public ProductStep(final Traversal.Admin traversal, final Object values) {
        super(traversal);

        if (values instanceof Traversal) {
            valueTraversal = integrateChild(((Traversal<S, E>) values).asAdmin());
        } else {
            parameterItems = values;
        }
    }

    @Override
    public String getStepName() { return "product"; }

    @Override
    protected List<List<?>> map(Traverser.Admin<S> traverser) {
        final Collection listA = convertTraverserToCollection(traverser);
        final Collection listB = (null != valueTraversal) ? convertTraversalToCollection(traverser, valueTraversal) : convertArgumentToCollection(parameterItems);
        final List elements = new ArrayList();

        for (Object elementInA : listA) {
            for (Object elementInB : listB) {
                final List pair = new ArrayList(2);
                pair.add(elementInA);
                pair.add(elementInB);
                elements.add(pair);
            }
        }

        return elements;
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return (null == valueTraversal) ? Collections.emptyList() : Collections.singletonList(valueTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() { return this.getSelfAndChildRequirements(); }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (valueTraversal != null) { this.integrateChild(this.valueTraversal); }
    }

    @Override
    public ProductStep<S, E> clone() {
        final ProductStep<S, E> clone = (ProductStep<S, E>) super.clone();
        if (null != this.valueTraversal) {
            clone.valueTraversal = this.valueTraversal.clone();
        } else {
            clone.parameterItems = this.parameterItems;
        }
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return Objects.hash(result, valueTraversal, parameterItems);
    }
}
