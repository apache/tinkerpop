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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

/**
 * Reference implementation for date difference step.
 *
 * @author Valentyn Kahamlyk
 */
public final class DateDiffStep<S> extends ScalarMapStep<S, Long> implements TraversalParent {

    private OffsetDateTime value;
    private Traversal.Admin<S, OffsetDateTime> dateTraversal;

    public DateDiffStep(final Traversal.Admin traversal, final OffsetDateTime value) {
        super(traversal);
        this.value = value;
    }

    public DateDiffStep(final Traversal.Admin traversal, final Traversal<?, OffsetDateTime> dateTraversal) {
        super(traversal);
        this.dateTraversal = this.integrateChild(dateTraversal.asAdmin());
    }

    @Override
    protected Long map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();

        if (!(object instanceof OffsetDateTime))
            throw new IllegalArgumentException(
                    String.format("DateDiff can only take DateTime as argument, encountered %s", object.getClass()));

        final OffsetDateTime otherDate = value != null ? value :
                dateTraversal != null ? TraversalUtil.apply(traverser, dateTraversal) : null;

        // let's not throw exception and assume null date == 0
        final long otherDateMs = otherDate == null ? 0 : otherDate.toEpochSecond();

        return (((OffsetDateTime) object).toEpochSecond() - otherDateMs);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : dateTraversal.hashCode());
        return result;
    }

    @Override
    public DateDiffStep<S> clone() {
        final DateDiffStep<S> clone = (DateDiffStep<S>) super.clone();
        clone.value = this.value;
        clone.dateTraversal = this.dateTraversal;
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    public OffsetDateTime getValue() {
        return this.value;
    }

    public Traversal.Admin<S, OffsetDateTime> getDateTraversal() {
        return this.dateTraversal;
    }
}
