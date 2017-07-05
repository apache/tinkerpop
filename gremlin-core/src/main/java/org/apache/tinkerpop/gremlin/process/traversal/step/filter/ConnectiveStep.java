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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ConnectiveStep<S> extends FilterStep<S> implements TraversalParent {

    public enum Connective {AND, OR}

    protected List<Traversal.Admin<S, ?>> traversals;

    public ConnectiveStep(final Traversal.Admin traversal, final Traversal<S, ?>... traversals) {
        super(traversal);
        this.traversals = Stream.of(traversals).map(Traversal::asAdmin).collect(Collectors.toList());
        this.traversals.forEach(this::integrateChild);
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return this.traversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    public void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        this.traversals.add(this.integrateChild((Traversal.Admin) localChildTraversal));
    }

    @Override
    public ConnectiveStep<S> clone() {
        final ConnectiveStep<S> clone = (ConnectiveStep<S>) super.clone();
        clone.traversals = new ArrayList<>();
        for (final Traversal.Admin<S, ?> traversal : this.traversals) {
            clone.traversals.add(traversal.clone());
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (final Traversal.Admin<S, ?> traversal : this.traversals) {
            integrateChild(traversal);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversals);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.traversals.hashCode();
    }
}