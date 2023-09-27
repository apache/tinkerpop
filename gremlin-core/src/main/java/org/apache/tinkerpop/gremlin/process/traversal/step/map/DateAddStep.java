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

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reference implementation for date concatenation step.
 *
 * @author Valentyn Kahamlyk
 */
public final class DateAddStep<S> extends ScalarMapStep<S, Date> {

    final static Map<DT, Integer> DTtoCalendar = new HashMap<>();

    static {
        DTtoCalendar.put(DT.second, Calendar.SECOND);
        DTtoCalendar.put(DT.minute, Calendar.MINUTE);
        DTtoCalendar.put(DT.hour, Calendar.HOUR_OF_DAY);
        DTtoCalendar.put(DT.day, Calendar.DAY_OF_MONTH);
    }

    private DT dateToken;
    private int value;

    public DateAddStep(final Traversal.Admin traversal, final DT dateToken, final int value) {
        super(traversal);
        this.dateToken = dateToken;
        this.value = value;
    }

    @Override
    protected Date map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();

        if (!(object instanceof Date)) throw new IllegalArgumentException("dateAdd accept only Date.");

        final Calendar cal = Calendar.getInstance();
        cal.setTime((Date) object);
        cal.add(DTtoCalendar.get(dateToken), value);

        return cal.getTime();
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
}
