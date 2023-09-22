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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Reference implementation for date parsing step.
 *
 * @author Valentyn Kahamlyk
 */
public final class AsDateStep<S> extends ScalarMapStep<S, Date> {

    public AsDateStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Date map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        if (object == null)
            throw new IllegalArgumentException("Can't parse null as Date.");
        if (object instanceof Date)
            return (Date) object;
        if (object instanceof Number)
            // numbers handled as milliseconds since January 1, 1970, 00:00:00 GMT.
            return new Date(((Number) object).longValue());
        if (object instanceof String) {
            try {
                return DatetimeHelper.parse((String) object);
            }
            catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Can't parse " + object + " as Date.");
            }
        }

        throw new IllegalArgumentException("Can't parse " + object.getClass().getName() + " as Date.");
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
    public String toString() {
        return StringFactory.stepString(this);
    }
}
