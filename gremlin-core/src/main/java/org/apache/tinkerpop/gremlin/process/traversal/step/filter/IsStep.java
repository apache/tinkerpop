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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IsStep<S> extends FilterStep<S> {

    private P<S> predicate;

    public IsStep(final Traversal.Admin traversal, final P<S> predicate) {
        super(traversal);
        this.predicate = predicate;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return this.predicate.test(traverser.get());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.predicate);
    }


    public P<S> getPredicate() {
        return this.predicate;
    }

    @Override
    public IsStep<S> clone() {
        final IsStep<S> clone = (IsStep<S>) super.clone();
        clone.predicate = this.predicate.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.predicate.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

}
