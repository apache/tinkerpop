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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.P;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class IsStep<S> extends FilterStep<S> {

    private final List<P<S>> predicates;
    // todo: boolean isSizeOne for optimization

    public IsStep(final Traversal.Admin traversal, final P<S>... predicates) {
        super(traversal);
        this.predicates = Arrays.asList(predicates);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        final S s = traverser.get();
        for (final P<S> predicate : this.predicates) {
            if (!predicate.test(s))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.predicates);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    public List<P<S>> getPredicates() {
        return this.predicates;
    }
}
