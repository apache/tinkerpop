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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Reference implementation for date difference step.
 *
 * @author Valentyn Kahamlyk
 */
public final class DateDiffStep<S> extends ScalarMapStep<S, Long> implements TraversalParent {

    private OffsetDateTime value;
    private Traversal.Admin<S, ?> dateTraversal;

    public DateDiffStep(final Traversal.Admin traversal, final Date value) {
        super(traversal);
        this.value = value == null ? null : value.toInstant().atOffset(ZoneOffset.UTC);
    }

    public DateDiffStep(final Traversal.Admin traversal, final OffsetDateTime value) {
        super(traversal);
        this.value = value;
    }

    public DateDiffStep(final Traversal.Admin traversal, final Traversal<?, ?> dateTraversal) {
        super(traversal);
        this.dateTraversal = this.integrateChild(dateTraversal.asAdmin());
    }

    @Override
    protected Long map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        OffsetDateTime date;
        OffsetDateTime otherDate;

        if (!(object instanceof OffsetDateTime)) {
            // allow incoming traverser to resolve into Date object for compatibility
            if (object instanceof Date) {
                date = ((Date) object).toInstant().atOffset(ZoneOffset.UTC);
            } else {
                // note: null handling consistency to be resolved https://issues.apache.org/jira/browse/TINKERPOP-3152
                throw new IllegalArgumentException(
                        String.format("DateDiff can only take OffsetDateTime or Date (deprecated) as argument, encountered %s", object == null ? null : object.getClass()));
            }
        } else {
            date = (OffsetDateTime) object;
        }


        if (null == value && null != dateTraversal) {
            Object traversalDate = TraversalUtil.apply(traverser, dateTraversal);
            if (traversalDate == null) {
                otherDate = null;
            } else if (traversalDate instanceof Date) {
                // for cases when traversal resolves to a java.util.Date object (e.g. inject(new Date()))
                otherDate = ((Date) traversalDate).toInstant().atOffset(ZoneOffset.UTC);
            } else if (traversalDate instanceof OffsetDateTime) {
                otherDate = (OffsetDateTime) traversalDate;
            } else {
                throw new IllegalArgumentException(String.format("DateDiff can only take OffsetDateTime or Date (deprecated) as argument, encountered %s", object.getClass()));
            }
        } else {
            otherDate = value;
        }

        // let's not throw exception and assume null date == 0
        return otherDate == null ? date.toInstant().toEpochMilli() : Duration.between(otherDate, date).toMillis();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        return dateTraversal == null ? Collections.emptyList() : List.of(dateTraversal);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (dateTraversal != null) {
            integrateChild(dateTraversal);
        }
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
        clone.dateTraversal = null == this.dateTraversal ? null : this.dateTraversal.clone();
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    public OffsetDateTime getValue() {
        return this.value;
    }

    public Traversal.Admin<S, ?> getDateTraversal() {
        return this.dateTraversal;
    }
}
