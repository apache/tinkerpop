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

import org.apache.tinkerpop.gremlin.process.traversal.GremlinTypeErrorException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

public final class AnyStep<S, S2> extends FilterStep<S> {

    private P<S2> predicate;

    public AnyStep(final Traversal.Admin traversal, final P<S2> predicate) {
        super(traversal);

        if (null == predicate) {
            throw new IllegalArgumentException("Input predicate to any step can't be null.");
        }

        this.predicate = predicate;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();

        if (item instanceof Iterable || item instanceof Iterator || ((item != null) && item.getClass().isArray())) {
            GremlinTypeErrorException typeError = null;
            final Iterator<S2> iterator = IteratorUtils.asIterator(item);
            while (iterator.hasNext()) {
                try {
                    if (this.predicate.test(iterator.next())) {
                        return true;
                    }
                } catch (GremlinTypeErrorException gtee) {
                    // hold onto it until the end in case any other element evaluates to TRUE
                    typeError = gtee;
                }
            }

            if (typeError != null) throw typeError;
        }

        return false;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.predicate);
    }

    @Override
    public AnyStep<S, S2> clone() {
        final AnyStep<S, S2> clone = (AnyStep<S, S2>) super.clone();
        clone.predicate = this.predicate.clone();
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }
}
