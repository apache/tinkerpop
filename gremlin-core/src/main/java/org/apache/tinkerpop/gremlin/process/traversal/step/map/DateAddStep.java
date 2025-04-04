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

import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for date concatenation step.
 *
 * @author Valentyn Kahamlyk
 */
public final class DateAddStep<S> extends ScalarMapStep<S, OffsetDateTime> {

    private DT dateToken;
    private int value;

    public DateAddStep(final Traversal.Admin traversal, final DT dateToken, final int value) {
        super(traversal);
        this.dateToken = dateToken;
        this.value = value;
    }

    @Override
    protected OffsetDateTime map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();

        if (!(object instanceof OffsetDateTime)) throw new IllegalArgumentException("dateAdd accept only DateTime.");

        OffsetDateTime date = (OffsetDateTime) object;
        OffsetDateTime new_date;

        switch (dateToken) {
            case second:
                new_date = date.plus(Duration.ofSeconds(value));
                break;
            case minute:
                new_date = date.plus(Duration.ofMinutes(value));
                break;
            case hour:
                new_date = date.plus(Duration.ofHours(value));
                break;
            case day:
                new_date = date.plus(Duration.ofDays(value));
                break;
            default:
                throw new IllegalArgumentException("DT tokens should only be second, minute, hour, or day.");
        }

        return new_date;
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
        result = 31 * result + dateToken.hashCode();
        result = 31 * result + value;
        return result;
    }

    @Override
    public DateAddStep<S> clone() {
        final DateAddStep<S> clone = (DateAddStep<S>) super.clone();
        clone.value = this.value;
        clone.dateToken = this.dateToken;
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    public DT getDateToken() {
        return this.dateToken;
    }

    public int getValue() {
        return this.value;
    }
}
